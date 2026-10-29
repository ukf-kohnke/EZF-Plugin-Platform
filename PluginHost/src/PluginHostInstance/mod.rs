//! Explicitly configured, independently owned plugin-host runtime instance.
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
    sync::Mutex,
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
/// state. The separate `Factory` module retains a compatibility facade over one default host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PluginOperationKind {
    Loading,
    Unloading,
}

pub struct PluginHostInstance {
    pub(super) inner: Mutex<PluginManager>,
    pub(super) config: PluginHostConfig,
    database_executor: Mutex<Option<PluginDatabaseCapability>>,
    operations: Mutex<BTreeMap<PathBuf, PluginOperationKind>>,
    /// Only the deprecated module-level facade retains legacy environment-per-load semantics.
    /// Explicitly constructed hosts never re-read process environment at execution time.
    legacy_environment: bool,
}

impl PluginHostInstance {
    pub fn new(config: PluginHostConfig) -> Self {
        Self {
            inner: Mutex::new(PluginManager::new()),
            config,
            database_executor: Mutex::new(None),
            operations: Mutex::new(BTreeMap::new()),
            legacy_environment: false,
        }
    }

    pub(crate) fn _new_legacy(config: PluginHostConfig) -> Self {
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
        // Repeated legacy registration is a no-op once a callback is installed. This preserves
        // existing Factory callers and cannot replace the capability captured by loaded plugins.
        {
            let slot = self.database_executor.lock().unwrap_or_else(|p| p.into_inner());
            if slot.is_some() {
                return Ok(());
            }
        }

        // Serialize first-time registration with load/unload reservations. A plugin receives a
        // snapshot of the host capability at load time; racing registration against a load must
        // not make the callback availability depend on timing.
        let operations = self.operations.lock().unwrap_or_else(|p| p.into_inner());
        if !operations.is_empty() {
            return Err(
                "cannot register the database capability while plugin load/unload operations are in progress".to_owned()
            );
        }

        let manager = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if manager.plugin_count() > 0 {
            return Err("database capability must be registered before loading plugins".to_owned());
        }
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

#[cfg(test)]
mod host_isolation_tests {
    use super::{
        PluginExecutorSelection,
        PluginHostInstance,
        PluginHostConfig,
        PluginRuntimeConfig,
    };
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
        let first = PluginHostInstance::new(
            config("/tmp/ezf-host-first", PluginExecutorSelection::InProcess)
        );
        let second = PluginHostInstance::new(
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
    #[test]
    fn database_capability_registration_is_blocked_during_lifecycle_operations() {
        let host = PluginHostInstance::new(
            config("/tmp/ezf-host-guard", PluginExecutorSelection::InProcess)
        );
        host.operations
            .lock()
            .unwrap()
            .insert(
                PathBuf::from("/tmp/ezf-host-guard/plugin.so"),
                super::PluginOperationKind::Loading
            );
        let result = host.register_database_executor(test_database_execute, test_database_free);
        assert!(result.is_err(), "capability registration must not race a plugin load");
    }
}
