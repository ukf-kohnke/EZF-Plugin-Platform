//! Public Factory facade for plugin loading, routing, database capabilities, and health.
mod types;
mod loading;
mod readiness;
mod database;
mod compatibility;
mod catalog;
mod sessions;
mod endpoints;

use crate::emit_log;
use super::PluginManager::{
    DatabasePluginSelection,
    PluginEndpointRoute,
    PluginManager,
    PluginProxy,
    PluginUnloadOutcome,
};
pub use super::PluginManager::{
    PluginExecutorKind,
    PluginHealthSnapshot,
    PluginHealthState,
    PluginRuntimeConfig,
};
use PluginContracts::{
    IPlugin::IPlugin,
    ApiContract::{
        normalize_plugin_path,
        PluginCancellation,
        PluginCatalog,
        PluginCatalogError,
        PluginDatabaseCapability,
        PluginEndpointError,
        PluginEndpointRequest,
        PluginManifest,
        PluginEndpointResponse,
        PluginFreeBufferFn,
        PluginLifecycleError,
        PluginSessionContext,
    },
};
use glob::PatternError;
use std::{
    collections::BTreeMap,
    fs,
    panic::{ AssertUnwindSafe, catch_unwind },
    path::{ Path, PathBuf },
    sync::{ Mutex, OnceLock },
};
pub use self::types::{
    PluginLoadFailure,
    PluginLoadPolicy,
    PluginLoadReport,
    PluginManagerError,
    PluginRequirement,
    PluginSessionNotificationFailure,
    PluginSessionNotificationReport,
};
use self::types::normalize_plugin_name;
pub use self::loading::{
    is_plugin_loaded,
    load_plugin_file,
    load_plugin_file_with_config,
    load_plugin_if_missing,
    load_plugins,
    load_plugins_with_policy,
    load_plugins_with_policy_and_config,
    unload_plugin,
};
pub use self::readiness::{ check_readiness, plugin_health, plugin_health_by_name };
pub use self::database::{
    check_database_readiness,
    execute_database_raw,
    has_database_plugin,
    load_database_plugins,
    load_database_plugins_with_config,
    register_database_executor,
    with_database_plugin,
};
pub use self::compatibility::{ with_all_plugins, with_plugin_by_name };
pub use self::catalog::{ plugin_catalog, plugin_manifests, plugin_names_for_endpoints };
pub use self::sessions::notify_session_closed;
pub use self::endpoints::{
    handle_endpoint,
    has_plugins,
    loaded_plugin_ids,
    loaded_plugin_names,
    plugin_count,
};

/// Selects the plugin call execution backend. This is part of the host configuration rather
/// than a value read each time a plugin is loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginExecutorSelection {
    InProcess,
    Process,
}

/// Explicit configuration for one plugin-host instance.
///
/// A host created with this configuration does not consult `ApplicationConfig`, the process-wide
/// plugin registry, or a process-global database callback. Plugin root, runtime limits, and
/// executor selection are captured here. The process executor's OS-sandbox launch policy remains
/// deployment configuration read from its documented `EZF_PLUGIN_SANDBOX_*` environment values.
#[derive(Debug, Clone)]
pub struct PluginHostConfig {
    pub plugin_root: PathBuf,
    pub runtime: PluginRuntimeConfig,
    pub executor: PluginExecutorSelection,
}

impl PluginHostConfig {
    pub fn from_environment() -> Self {
        let executor = match std::env::var("EZF_PLUGIN_EXECUTOR").as_deref() {
            Ok("process") => PluginExecutorSelection::Process,
            _ => PluginExecutorSelection::InProcess,
        };
        Self {
            plugin_root: default_plugin_root(),
            runtime: PluginRuntimeConfig::from_environment(),
            executor,
        }
    }
}

impl Default for PluginHostConfig {
    fn default() -> Self {
        Self::from_environment()
    }
}

/// One independent plugin-host instance.
///
/// Registry/catalog state, database capability, and per-library lifecycle-operation guards are
/// all owned by this object. Independent instances can therefore use different plugin roots,
/// runtime limits, executor backends, and database callbacks without overwriting each other's
/// state. The `Factory` module functions below remain a compatibility facade over a lazily-created
/// default host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PluginOperationKind {
    Loading,
    Unloading,
}

pub struct PluginHost {
    pub(super) inner: Mutex<PluginManager>,
    pub(super) config: PluginHostConfig,
    database_executor: Mutex<Option<PluginDatabaseCapability>>,
    operations: Mutex<BTreeMap<PathBuf, PluginOperationKind>>,
    /// Only the deprecated module-level facade retains legacy environment-per-load semantics.
    /// Explicitly constructed hosts never re-read process environment at execution time.
    legacy_environment: bool,
}

impl PluginHost {
    pub fn new(config: PluginHostConfig) -> Self {
        Self {
            inner: Mutex::new(PluginManager::new()),
            config,
            database_executor: Mutex::new(None),
            operations: Mutex::new(BTreeMap::new()),
            legacy_environment: false,
        }
    }

    fn new_legacy(config: PluginHostConfig) -> Self {
        let mut host = Self::new(config);
        host.legacy_environment = true;
        host
    }

    pub(super) fn executor_selection_for_load(&self) -> Result<PluginExecutorSelection, String> {
        if !self.legacy_environment {
            return Ok(self.config.executor);
        }
        match std::env::var("EZF_PLUGIN_EXECUTOR") {
            Ok(mode) if mode == "process" => Ok(PluginExecutorSelection::Process),
            Ok(mode) if mode == "in_process" => Ok(PluginExecutorSelection::InProcess),
            Err(std::env::VarError::NotPresent) => Ok(PluginExecutorSelection::InProcess),
            Ok(other) =>
                Err(
                    format!(
                        "unknown EZF_PLUGIN_EXECUTOR mode {other:?}; expected 'in_process' or 'process'"
                    )
                ),
            Err(error) => Err(format!("could not read EZF_PLUGIN_EXECUTOR: {error}")),
        }
    }

    pub fn config(&self) -> &PluginHostConfig {
        &self.config
    }

    pub fn register_database_executor(
        &self,
        executor: PluginContracts::ApiContract::PluginDatabaseExecuteFn,
        free: PluginFreeBufferFn
    ) -> Result<(), String> {
        let mut slot = self.database_executor.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            *slot = Some(PluginDatabaseCapability { execute: executor, free });
        }
        Ok(())
    }

    pub(super) fn database_capability(&self) -> Option<PluginDatabaseCapability> {
        *self.database_executor.lock().unwrap_or_else(|p| p.into_inner())
    }
}

fn default_plugin_root() -> PathBuf {
    if let Some(root) = std::env::var_os("EZF_PLUGIN_ROOT") {
        return PathBuf::from(root);
    }
    let mut path = PathBuf::from(std::env::var_os("EZFBACKEND_HOME").unwrap_or_default());
    path.push("deployment");
    #[cfg(debug_assertions)]
    path.push("debug");
    #[cfg(not(debug_assertions))]
    path.push("release");
    path.push("plugins");
    path
}

fn instance() -> &'static PluginHost {
    static INSTANCE: OnceLock<PluginHost> = OnceLock::new();
    INSTANCE.get_or_init(|| PluginHost::new_legacy(PluginHostConfig::from_environment()))
}

#[cfg(test)]
mod host_isolation_tests {
    use super::{ PluginExecutorSelection, PluginHost, PluginHostConfig, PluginRuntimeConfig };
    use std::path::PathBuf;

    fn config(root: &str, executor: PluginExecutorSelection) -> PluginHostConfig {
        PluginHostConfig {
            plugin_root: PathBuf::from(root),
            runtime: PluginRuntimeConfig::default(),
            executor,
        }
    }

    unsafe extern "C" fn test_database_execute(
        _request: PluginAbi::AbiSlice,
        _cancellation: PluginAbi::PluginCancellationV2,
        _response: *mut PluginAbi::AbiBuffer
    ) -> i32 {
        0
    }

    unsafe extern "C" fn test_database_free(_buffer: PluginAbi::AbiBuffer) {}

    #[test]
    fn host_instances_keep_configuration_registry_and_capabilities_separate() {
        let first = PluginHost::new(
            config("/tmp/ezf-host-first", PluginExecutorSelection::InProcess)
        );
        let second = PluginHost::new(
            config("/tmp/ezf-host-second", PluginExecutorSelection::Process)
        );

        assert_eq!(first.config().plugin_root, PathBuf::from("/tmp/ezf-host-first"));
        assert_eq!(second.config().plugin_root, PathBuf::from("/tmp/ezf-host-second"));
        assert_eq!(first.config().executor, PluginExecutorSelection::InProcess);
        assert_eq!(second.config().executor, PluginExecutorSelection::Process);
        assert_eq!(first.plugin_count(), 0);
        assert_eq!(second.plugin_count(), 0);

        first.register_database_executor(test_database_execute, test_database_free).unwrap();
        assert!(first.database_capability().is_some());
        assert!(second.database_capability().is_none());
        assert!(first.operations.lock().unwrap().is_empty());
        assert!(second.operations.lock().unwrap().is_empty());
    }
}
