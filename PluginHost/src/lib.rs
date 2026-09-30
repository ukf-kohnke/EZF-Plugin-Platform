//! Factory and singleton access layer for runtime plugin management.
//!
//! This module provides a thread-safe, process-wide entry point to load, query,
//! execute, and unload plugins through an internal `PluginManager` guarded by
//! `Mutex` + `OnceLock`.
//!
//! # Responsibilities
//!
//! - Lazily initialize a single `PluginManager` instance.
//! - Load one plugin file or scan/load many plugin files from a directory pattern.
//! - Check whether a plugin is already loaded (by plugin runtime name).
//! - Resolve plugin IDs by name.
//! - Execute endpoint requests (`handle_endpoint`).
//! - Broadcast sanitized backend-session lifecycle events to loaded plugins.
//! - Expose inspection helpers (loaded names/ids, count, catalog, `has_plugins`).
//! - Unload a single plugin or all plugins.
//!
//! # Error model
//!
//! Plugin loading/unloading uses `PluginManagerError`, which captures:
//!
//! - catalog validation errors,
//! - invalid paths and glob patterns,
//! - dynamic library / constructor symbol loading failures,
//! - runtime/manifest name mismatches,
//! - initialization failures,
//! - already-loaded / not-found / unload failures.
//!
//! `PluginManagerError` implements `Display` and `std::error::Error`, forwarding
//! nested sources where available.
//!
//! Endpoint execution maps loading/lookup failures to `PluginEndpointError` with
//! stable error codes/messages suitable for API responses.
//!
//! # Concurrency notes
//!
//! The global mutex protects only plugin-manager state (registration, lookup,
//! catalog and lifecycle bookkeeping). Plugin execution is synchronized by a
//! per-plugin mutex after the manager lock has been released. This is important
//! because a plugin may legitimately call back into the factory for another
//! plugin, for example a domain plugin using the database plugin.
//!
//! # Behavior notes
//!
//! - `load_plugin_if_missing` attempts lazy loading from the plugin root path.
//! - Name-based lookup is the canonical way to select a plugin at runtime.
//! - `with_plugin_by_name`, `with_all_plugins`, and `with_database_plugin` provide scoped mutable
//!   access to plugin instances via closures. Database execution is selected by manifest capability.
//!
//! # Return conventions
//!
//! - `is_plugin_loaded` / `load_plugin_if_missing`: `Ok(true)` when plugin is
//!   available after optional lazy loading.
//! - `with_*` helpers: `None` when no matching plugin (or no plugins) is present.
//! - bulk load/unload functions return counts of affected plugins.
#![allow(non_snake_case)]
mod PluginManager;
pub mod Factory {
    use super::PluginManager::{ PluginManager, PluginProxy };
    use PluginContracts::{
        IPlugin::IPlugin,
        ApiContract::{
            PluginCatalog,
            PluginCatalogError,
            PluginDatabaseCapability,
            PluginEndpointError,
            PluginEndpointRequest,
            PluginEndpointResponse,
            PluginFreeBufferFn,
            PluginKind,
            PluginLifecycleError,
            PluginSessionContext,
        },
    };
    use Common::Helper::Functions::getPluginRootDir;
    use glob::PatternError;
    use std::{
        fs,
        panic::{ AssertUnwindSafe, catch_unwind },
        path::{ Path, PathBuf },
        sync::{ Mutex, OnceLock },
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct PluginSessionNotificationFailure {
        pub plugin_name: String,
        pub error: PluginLifecycleError,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct PluginSessionNotificationReport {
        pub notified_plugins: Vec<String>,
        pub failures: Vec<PluginSessionNotificationFailure>,
    }

    impl PluginSessionNotificationReport {
        pub fn is_success(&self) -> bool {
            self.failures.is_empty()
        }
    }

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

    #[derive(Debug)]
    pub enum PluginManagerError {
        InvalidCatalog(PluginCatalogError),
        InvalidPluginPath(PathBuf),
        InvalidGlobPattern {
            pattern: String,
            source: PatternError,
        },
        LibraryLoad {
            path: PathBuf,
            source: libloading::Error,
        },
        ConstructorLoad {
            path: PathBuf,
            symbol: &'static str,
            source: libloading::Error,
        },
        PluginAbiIncompatible {
            path: PathBuf,
            message: String,
        },
        PluginAbiCall {
            path: PathBuf,
            message: String,
        },
        PluginNameMismatch {
            path: PathBuf,
            runtime_name: String,
            manifest_name: String,
        },
        PluginInitializationFailed {
            path: PathBuf,
        },
        PluginAlreadyLoaded {
            path: PathBuf,
            id: i64,
        },
        PluginNotFound {
            id: i64,
        },
        PluginUnloadFailed {
            id: i64,
        },
    }

    impl std::fmt::Display for PluginManagerError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                PluginManagerError::InvalidCatalog(error) =>
                    write!(f, "Invalid catalog configuration: {:?}", error),
                PluginManagerError::InvalidPluginPath(path) =>
                    write!(f, "Invalid plugin path: {:?}", path),
                PluginManagerError::InvalidGlobPattern { pattern, source } =>
                    write!(f, "Invalid glob pattern '{}': {}", pattern, source),
                PluginManagerError::LibraryLoad { path, source } =>
                    write!(f, "Failed to load library at {:?}: {}", path, source),
                PluginManagerError::ConstructorLoad { path, symbol, source } =>
                    write!(f, "Failed to load constructor '{}' at {:?}: {}", symbol, path, source),
                PluginManagerError::PluginAbiIncompatible { path, message } =>
                    write!(f, "Plugin at {:?} exposes an incompatible ABI: {}", path, message),
                PluginManagerError::PluginAbiCall { path, message } =>
                    write!(f, "Plugin ABI call failed at {:?}: {}", path, message),
                PluginManagerError::PluginNameMismatch { path, runtime_name, manifest_name } =>
                    write!(
                        f,
                        "Failed to match plugin '{:?}' , {:?} : {}",
                        path,
                        runtime_name,
                        manifest_name
                    ),
                PluginManagerError::PluginInitializationFailed { path } =>
                    write!(f, "Plugin initialization failed at {:?}", path),
                PluginManagerError::PluginAlreadyLoaded { path, id } =>
                    write!(f, "Plugin already loaded at {:?} with id {}", path, id),
                PluginManagerError::PluginNotFound { id } =>
                    write!(f, "Plugin not found with id {}", id),
                PluginManagerError::PluginUnloadFailed { id } =>
                    write!(f, "Failed to unload plugin with id {}", id),
            }
        }
    }

    impl std::error::Error for PluginManagerError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                PluginManagerError::InvalidGlobPattern { source, .. } => Some(source),
                PluginManagerError::LibraryLoad { source, .. } => Some(source),
                PluginManagerError::ConstructorLoad { source, .. } => Some(source),
                _ => None,
            }
        }
    }

    struct SingletonReader {
        inner: Mutex<PluginManager>,
    }

    fn instance() -> &'static SingletonReader {
        static INSTANCE: OnceLock<SingletonReader> = OnceLock::new();
        INSTANCE.get_or_init(|| SingletonReader {
            inner: Mutex::new(PluginManager::new()),
        })
    }

    #[allow(unused)]
    pub(crate) fn unload_plugin(id: i64) -> Result<(), PluginManagerError> {
        // Detach under the manager lock, execute on_unload() without it, and only restore on failure.
        let mut item = {
            let singleton = instance();
            let mut manager = singleton.inner.lock().unwrap();
            manager.detach_plugin(id)?
        };

        if item.unload() {
            drop(item);
            Ok(())
        } else {
            let singleton = instance();
            let mut manager = singleton.inner.lock().unwrap();
            manager.restore_plugin(id, item);
            Err(PluginManagerError::PluginUnloadFailed { id })
        }
    }

    pub(crate) fn is_plugin_loaded(
        plugin_name: &str,
        plugin_path: &str
    ) -> Result<bool, PluginManagerError> {
        if find_id_by_name(plugin_name).is_some() {
            return Ok(true);
        }

        let len = load_plugins(plugin_path)?;
        Ok(len > 0 && find_id_by_name(plugin_name).is_some())
    }

    pub(crate) fn find_id_by_name(plugin_name: &str) -> Option<i64> {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap();

        manager
            .plugin_ids()
            .into_iter()
            .find(|id|
                manager.get_manifest(*id).is_some_and(|manifest| manifest.name == plugin_name)
            )
    }

    pub fn load_plugin_file(path: &Path) -> Result<i64, PluginManagerError> {
        let canonical_path = PluginManager::normalize_path(path);

        // Avoid invoking plugin code at all when the path is already registered. The check is
        // repeated after load to cover concurrent load attempts.
        {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap();
            if let Some(existing_id) = manager.paths.get(&canonical_path).copied() {
                return Err(PluginManagerError::PluginAlreadyLoaded {
                    path: canonical_path,
                    id: existing_id,
                });
            }
        }

        let loaded = PluginManager::load_plugin(&canonical_path)?;
        let plugin_manifest = loaded.manifest().clone();

        let result = {
            let singleton = instance();
            let mut manager = singleton.inner.lock().unwrap();

            if let Some(existing_id) = manager.paths.get(&canonical_path).copied() {
                Err(PluginManagerError::PluginAlreadyLoaded {
                    path: canonical_path.clone(),
                    id: existing_id,
                })
            } else {
                let catalog = manager.catalog_with_manifest(&plugin_manifest)?;
                Ok(manager.insert_loaded_plugin(&canonical_path, loaded, catalog))
            }
        };

        // On a duplicate or catalog error `loaded` was not consumed and is therefore dropped here,
        // outside the manager lock, so its on_unload() callback cannot re-enter the locked manager.
        result
    }

    pub fn load_plugins(plugin_path: &str) -> Result<usize, PluginManagerError> {
        let entries = PluginManager::discover_plugin_paths(plugin_path)?;
        let mut loaded_count = 0usize;
        let mut had_entries = false;

        for entry in entries {
            let canonical_path = match entry {
                Ok(path) => {
                    had_entries = true;
                    path
                }
                Err(error) => {
                    Common::Logging::Logger::Log::Dump(
                        "{}",
                        Common::Logging::LogConstants::LOGGERDEF,
                        log::LevelFilter::Error,
                        &[&format!("Plugin scan failed: {error}")]
                    );
                    continue;
                }
            };

            {
                let singleton = instance();
                let manager = singleton.inner.lock().unwrap();
                if manager.paths.contains_key(&canonical_path) {
                    continue;
                }
            }

            let loaded = match PluginManager::load_plugin(&canonical_path) {
                Ok(loaded) => loaded,
                Err(error) => {
                    Common::Logging::Logger::Log::Dump(
                        "{}",
                        Common::Logging::LogConstants::LOGGERDEF,
                        log::LevelFilter::Error,
                        &[&format!("Plugin loading failed: {error}")]
                    );
                    continue;
                }
            };
            let manifest = loaded.manifest().clone();

            let registration = {
                let singleton = instance();
                let mut manager = singleton.inner.lock().unwrap();

                if let Some(existing_id) = manager.paths.get(&canonical_path).copied() {
                    Err((
                        PluginManagerError::PluginAlreadyLoaded {
                            path: canonical_path.clone(),
                            id: existing_id,
                        },
                        loaded,
                    ))
                } else {
                    match manager.catalog_with_manifest(&manifest) {
                        Ok(catalog) =>
                            Ok(manager.insert_loaded_plugin(&canonical_path, loaded, catalog)),
                        Err(error) => Err((error, loaded)),
                    }
                }
            };

            match registration {
                Ok(_) => {
                    loaded_count += 1;
                }
                Err((error, _loaded)) => {
                    Common::Logging::Logger::Log::Dump(
                        "{}",
                        Common::Logging::LogConstants::LOGGERDEF,
                        log::LevelFilter::Error,
                        &[&format!("Plugin registration failed: {error}")]
                    );
                }
            }
        }

        if loaded_count == 0 && !had_entries {
            Common::Logging::Logger::Log::Dump(
                "{}",
                Common::Logging::LogConstants::LOGGERDEF,
                log::LevelFilter::Warn,
                &["No plugins were found in the configured plugin directory."]
            );
        }

        Ok(loaded_count)
    }

    pub fn load_plugin_if_missing(
        plugin_name: &str,
        plugin_path: &str
    ) -> Result<bool, PluginManagerError> {
        is_plugin_loaded(plugin_name, plugin_path)
    }

    /// Returns whether a database-capable data-source plugin is currently loaded.
    ///
    /// Database execution is selected by the plugin manifest kind rather than by a
    /// concrete plugin name. This keeps the infrastructure boundary independent of
    /// MolgenDB or any other specific database implementation.
    pub fn has_database_plugin() -> bool {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap();

        manager
            .plugin_ids()
            .into_iter()
            .any(|id| {
                manager
                    .get_manifest(id)
                    .is_some_and(|manifest| manifest.kind == PluginKind::DataSource)
            })
    }

    /// Executes a callback against the unique loaded data-source plugin.
    ///
    /// A database invocation is intentionally rejected when zero or multiple
    /// data-source plugins are loaded. Silent first-match selection would make the
    /// active database depend on plugin discovery order.
    pub fn with_database_plugin<R, F>(func: F) -> Result<R, String>
        where F: FnOnce(&mut dyn IPlugin) -> R
    {
        let (runtimes, names) = {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap();

            let mut runtimes = Vec::new();
            let mut names = Vec::new();

            for id in manager.plugin_ids() {
                let Some(manifest) = manager.get_manifest(id) else {
                    continue;
                };
                if manifest.kind != PluginKind::DataSource {
                    continue;
                }

                let Some(runtime) = manager.get_runtime(id) else {
                    continue;
                };

                names.push(manifest.name.clone());
                runtimes.push(runtime);
            }

            (runtimes, names)
        };

        match runtimes.len() {
            0 => Err("no database data-source plugin is loaded".to_owned()),
            1 => {
                let mut proxy = PluginProxy::new(runtimes[0].clone());
                Ok(func(&mut proxy))
            }
            _ =>
                Err(
                    format!(
                        "multiple database data-source plugins are loaded: {}",
                        names.join(", ")
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
        let database_root = PathBuf::from(getPluginRootDir()).join("database");
        let entries = fs
            ::read_dir(&database_root)
            .map_err(|error|
                format!(
                    "failed to read database plugin root '{}': {error}",
                    database_root.display()
                )
            )?;

        let mut loaded_count = 0usize;
        for entry in entries {
            let entry = entry.map_err(|error|
                format!(
                    "failed to inspect database plugin root '{}': {error}",
                    database_root.display()
                )
            )?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let path = path.to_string_lossy().into_owned();
            loaded_count += load_plugins(&path).map_err(|error|
                format!("failed to load database plugins from '{}': {error}", path)
            )?;
        }

        Ok(loaded_count)
    }

    pub fn with_plugin_by_name<R, F>(plugin_name: &str, func: F) -> Option<R>
        where F: FnOnce(&mut dyn IPlugin) -> R
    {
        if plugin_name.is_empty() {
            return None;
        }

        let runtime = {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap();
            let id = manager
                .plugin_ids()
                .into_iter()
                .find(|id|
                    manager.get_manifest(*id).is_some_and(|manifest| manifest.name == plugin_name)
                )?;
            manager.get_runtime(id)?
        };

        {
            let mut proxy = PluginProxy::new(runtime);
            Some(func(&mut proxy))
        }
    }

    pub fn plugin_catalog() -> PluginCatalog {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap();

        let catalog = manager.catalog();
        catalog
    }

    pub fn with_all_plugins<R, F>(func: F) -> Option<Vec<R>> where F: Fn(&mut dyn IPlugin) -> R {
        let runtimes = {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap();
            let ids = manager.plugin_ids();
            if ids.is_empty() {
                return None;
            }

            ids.into_iter()
                .filter_map(|id| manager.get_runtime(id))
                .collect::<Vec<_>>()
        };

        let results = runtimes
            .into_iter()
            .filter_map(|runtime| {
                let mut proxy = PluginProxy::new(runtime);
                Some(func(&mut proxy))
            })
            .collect::<Vec<_>>();

        Some(results)
    }

    pub fn notify_session_closed(
        context: &PluginSessionContext
    ) -> PluginSessionNotificationReport {
        let runtimes = {
            let singleton = instance();
            let manager = match singleton.inner.lock() {
                Ok(manager) => manager,
                Err(poisoned) => poisoned.into_inner(),
            };

            manager
                .plugin_ids()
                .into_iter()
                .filter_map(|id| {
                    let name = manager.get_manifest(id)?.name.clone();
                    let runtime = manager.get_runtime(id)?;
                    Some((name, runtime))
                })
                .collect::<Vec<_>>()
        };

        let mut report = PluginSessionNotificationReport::default();

        for (plugin_name, runtime) in runtimes {
            let notification = catch_unwind(
                AssertUnwindSafe(|| {
                    let mut proxy = PluginProxy::new(runtime.clone());
                    proxy.on_session_closed(context)
                })
            );

            match notification {
                Ok(Ok(())) => {
                    report.notified_plugins.push(plugin_name);
                }
                Ok(Err(error)) => {
                    report.notified_plugins.push(plugin_name.clone());
                    report.failures.push(PluginSessionNotificationFailure {
                        plugin_name,
                        error,
                    });
                }
                Err(_) => {
                    report.failures.push(PluginSessionNotificationFailure {
                        plugin_name,
                        error: PluginLifecycleError::new(
                            "plugin_session_callback_panicked",
                            "The plugin panicked while processing a closed session"
                        ),
                    });
                }
            }
        }

        report
    }

    pub fn loaded_plugin_names() -> Vec<String> {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap();

        manager
            .plugin_ids()
            .into_iter()
            .filter_map(|id| manager.get_manifest(id).map(|manifest| manifest.name.to_string()))
            .collect()
    }

    pub fn loaded_plugin_ids() -> Vec<i64> {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap();
        manager.plugin_ids()
    }

    pub fn plugin_count() -> usize {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap();
        manager.plugin_count()
    }

    pub fn has_plugins() -> bool {
        plugin_count() > 0
    }

    pub fn handle_endpoint(
        plugin_name: &str,
        request: PluginEndpointRequest
    ) -> Result<PluginEndpointResponse, PluginEndpointError> {
        if
            !load_plugin_if_missing(plugin_name, &getPluginRootDir()).map_err(|error| {
                PluginEndpointError::internal(
                    "plugin_load_failed",
                    "Failed to load the specified plugin",
                    &format!("Failed to load plugin '{}': {}", plugin_name, error)
                )
            })?
        {
            return Err(
                PluginEndpointError::not_found(
                    "plugin_not_found",
                    "The specified plugin could not be found",
                    &format!("Plugin '{}' is not available in the system", plugin_name)
                )
            );
        }

        let runtime = {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap();
            let id = manager
                .plugin_ids()
                .into_iter()
                .find(|id|
                    manager.get_manifest(*id).is_some_and(|manifest| manifest.name == plugin_name)
                )
                .ok_or_else(||
                    PluginEndpointError::not_found(
                        "plugin_not_found",
                        "The specified plugin could not be found",
                        &format!("Plugin '{}' is not available in the system", plugin_name)
                    )
                )?;
            manager
                .get_runtime(id)
                .ok_or_else(||
                    PluginEndpointError::not_found(
                        "plugin_not_found",
                        "The specified plugin could not be found",
                        &format!("Plugin '{}' is not available in the system", plugin_name)
                    )
                )?
        };

        crate::PluginManager::PluginRuntime
            ::execute(&runtime, |plugin| plugin.handle_endpoint(request))
            .ok_or_else(||
                PluginEndpointError::not_found(
                    "plugin_not_found",
                    "The specified plugin could not be found",
                    &format!("Plugin '{}' is not available in the system", plugin_name)
                )
            )?
    }
}
