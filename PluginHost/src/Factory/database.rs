//! Internal Factory database operations.
use super::*;

static HOST_DATABASE_EXECUTOR: OnceLock<PluginDatabaseCapability> = OnceLock::new();

/// Registers the host-owned database capability used when plugin instances are constructed.
/// The capability is immutable after the first successful registration.
pub fn register_database_executor(
    executor: PluginContracts::ApiContract::PluginDatabaseExecuteFn,
    free: PluginFreeBufferFn
) -> Result<(), String> {
    if HOST_DATABASE_EXECUTOR.get().is_some() {
        return Ok(());
    }

    HOST_DATABASE_EXECUTOR.set(PluginDatabaseCapability { execute: executor, free }).map_err(|_|
        "plugin database executor was already initialized".to_owned()
    )
}

pub(crate) fn database_capability() -> Option<PluginDatabaseCapability> {
    HOST_DATABASE_EXECUTOR.get().copied()
}

/// Executes the standard read-only database readiness probe against the active data-source plugin.
///
/// The probe stays in the plugin host because database readiness is a runtime capability,
/// not an HTTP-layer concern. The API can therefore query readiness without depending on
/// the concrete `Infrastructure::Datasource::DatabaseExecutor` implementation.
pub fn check_database_readiness() -> Result<(), String> {
    let request = DatabaseContracts::DatabaseRequest::Query(DatabaseContracts::QueryRequest {
        statement: "SELECT 1".to_owned(),
        parameters: Vec::new(),
        columns: Vec::new(),
    });

    let result = with_database_plugin(|plugin| {
        plugin.execute_database(request, &PluginCancellation::uncancelled())
    })?;
    let result = result.map_err(|error| error.to_string())?;

    match result {
        DatabaseContracts::DatabaseResponse::Query(result_set) if result_set.rows.len() == 1 =>
            Ok(()),
        DatabaseContracts::DatabaseResponse::Query(result_set) =>
            Err(
                format!(
                    "database connectivity query returned {} rows instead of 1",
                    result_set.rows.len()
                )
            ),
        DatabaseContracts::DatabaseResponse::Execute(_) =>
            Err("database connectivity probe returned an execution result".to_owned()),
    }
}

/// Returns whether a database-capable data-source plugin is currently loaded.
///
/// Database execution is selected by the plugin manifest kind rather than by a
/// concrete plugin name. This keeps the infrastructure boundary independent of
/// MolgenDB or any other specific database implementation.
pub fn has_database_plugin() -> bool {
    let singleton = instance();
    let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    !matches!(manager.database_plugin_selection(), DatabasePluginSelection::None)
}

/// Executes a callback against the unique loaded data-source plugin.
///
/// A database invocation is intentionally rejected when zero or multiple
/// data-source plugins are loaded. Silent first-match selection would make the
/// active database depend on plugin discovery order.
pub fn with_database_plugin<R, F>(func: F) -> Result<R, String> where F: FnOnce(&dyn IPlugin) -> R {
    let selection = {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        manager.database_plugin_selection()
    };

    match selection {
        DatabasePluginSelection::None => Err("no database data-source plugin is loaded".to_owned()),
        DatabasePluginSelection::Unique { runtime } => {
            let proxy = PluginProxy::new(runtime);
            Ok(func(&proxy))
        }
        DatabasePluginSelection::Multiple { names } =>
            Err(format!("multiple database data-source plugins are loaded: {}", names.join(", "))),
    }
}

/// Executes a DataSource ABI operation with raw JSON request bytes and returns the exact
/// host-owned response bytes. Successful response envelopes are not deserialized and encoded
/// again on the host. Call-level failures remain typed so the Infrastructure capability can
/// preserve its established error mapping.
pub fn execute_database_raw(
    request_bytes: Vec<u8>,
    cancellation: PluginAbi::PluginCancellationV2
) -> Result<Vec<u8>, DatabaseContracts::DatabaseError> {
    let selection = {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        manager.database_plugin_selection()
    };

    match selection {
        DatabasePluginSelection::None =>
            Err(
                DatabaseContracts::DatabaseError::new(
                    DatabaseContracts::DatabaseErrorKind::Query,
                    "no database data-source plugin is loaded"
                )
            ),
        DatabasePluginSelection::Unique { runtime } =>
            runtime.execute_database_bytes_with_parent(request_bytes, cancellation),
        DatabasePluginSelection::Multiple { names } =>
            Err(
                DatabaseContracts::DatabaseError::new(
                    DatabaseContracts::DatabaseErrorKind::Query,
                    format!(
                        "multiple database data-source plugins are loaded: {}",
                        names.join(", ")
                    )
                )
            ),
    }
}

/// Loads all database plugins found in the runtime `plugins/database` tree.
///
/// Database plugins are deployed one directory below `plugins/database` so
/// that their configuration can live alongside the shared library. The host
/// deliberately discovers those directories instead of encoding a plugin name.
pub fn load_database_plugins() -> Result<usize, String> {
    load_database_plugins_with_config(PluginRuntimeConfig::from_environment())
}

/// Loads database plugins with the same runtime limits used for the rest of the API plugins.
/// Startup uses this form so every plugin in one backend instance has a consistent runtime policy.
pub fn load_database_plugins_with_config(config: PluginRuntimeConfig) -> Result<usize, String> {
    let database_root = PathBuf::from(plugin_root_dir()).join("database");
    let entries = fs
        ::read_dir(&database_root)
        .map_err(|error|
            format!("failed to read database plugin root '{}': {error}", database_root.display())
        )?;

    let mut directories = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error|
            format!("failed to inspect database plugin root '{}': {error}", database_root.display())
        )?;
        let path = entry.path();
        if path.is_dir() {
            directories.push(path);
        }
    }
    directories.sort();

    let mut loaded_count = 0usize;
    for path in directories {
        let path = path.to_string_lossy().into_owned();
        let report = load_plugins_with_policy_and_config(
            &path,
            PluginLoadPolicy::optional_all(),
            config
        ).map_err(|error| { format!("failed to load database plugins from '{}': {error}", path) })?;
        loaded_count += report.loaded_count();
    }

    Ok(loaded_count)
}
