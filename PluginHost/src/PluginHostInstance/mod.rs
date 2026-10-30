//! Explicitly configured, independently owned plugin-host runtime instance.
mod types;
mod process_config;
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
pub use self::process_config::{ ProcessExecutorConfig, SandboxConfig, WorkerEnvironment };
pub use PluginIpc::SandboxProfile;
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
    cell::RefCell,
    collections::BTreeMap,
    fs,
    panic::{ AssertUnwindSafe, catch_unwind },
    path::{ Path, PathBuf },
    sync::{ Arc, Mutex, Weak },
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
/// plugin registry, or a process-global database callback. Plugin root, runtime limits, executor
/// selection, and all worker/sandbox launch settings are captured here.
#[derive(Debug, Clone)]
pub struct PluginHostConfig {
    pub plugin_root: PathBuf,
    pub runtime: PluginRuntimeConfig,
    pub executor: PluginExecutorSelection,
    pub process_executor: ProcessExecutorConfig,
}

impl Default for PluginHostConfig {
    fn default() -> Self {
        Self {
            plugin_root: PathBuf::new(),
            runtime: PluginRuntimeConfig::default(),
            executor: PluginExecutorSelection::InProcess,
            process_executor: ProcessExecutorConfig::default(),
        }
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
    pub(super) inner: Arc<Mutex<PluginManager>>,
    pub(super) config: PluginHostConfig,
    database_executor: Mutex<Option<PluginDatabaseCapability>>,
    operations: Mutex<BTreeMap<PathBuf, PluginOperationKind>>,
}

/// Per-host database dispatch handle used by ABI callback trampolines.
///
/// The ABI V2 database callback has no user-data pointer. A thread-local scope therefore carries
/// this weak manager handle while plugin code is running (or while the process-mode capability
/// broker is serving a request). It avoids routing callbacks through the compatibility Factory
/// singleton while preserving the existing ABI V2 function-pointer contract.
#[derive(Clone)]
pub(crate) struct DatabaseDispatcher {
    manager: Weak<Mutex<PluginManager>>,
}

impl DatabaseDispatcher {
    fn new(manager: &Arc<Mutex<PluginManager>>) -> Self {
        Self { manager: Arc::downgrade(manager) }
    }

    fn execute_database_raw(
        &self,
        request_bytes: Vec<u8>,
        cancellation: PluginAbi::PluginCancellationV2
    ) -> Result<Vec<u8>, DatabaseContracts::DatabaseError> {
        let manager = self.manager
            .upgrade()
            .ok_or_else(||
                DatabaseContracts::DatabaseError::new(
                    DatabaseContracts::DatabaseErrorKind::Query,
                    "plugin host is no longer available for database dispatch"
                )
            )?;
        let selection = manager
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .database_plugin_selection();
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
}

thread_local! {
    static ACTIVE_DATABASE_DISPATCHER: RefCell<Option<DatabaseDispatcher>> = const {
        RefCell::new(None)
    };
}

/// Install the host-specific database dispatcher for the synchronous duration of a plugin call.
pub(crate) fn with_database_dispatcher<T>(
    dispatcher: DatabaseDispatcher,
    operation: impl FnOnce() -> T
) -> T {
    let previous = ACTIVE_DATABASE_DISPATCHER.with(|slot| slot.replace(Some(dispatcher)));
    struct Scope(Option<DatabaseDispatcher>);
    impl Drop for Scope {
        fn drop(&mut self) {
            ACTIVE_DATABASE_DISPATCHER.with(|slot| {
                let _ = slot.replace(self.0.take());
            });
        }
    }
    let _scope = Scope(previous);
    operation()
}

/// Database callback entry point for ABI V2 host-service trampolines.
/// It must be called only from inside an active host plugin invocation/capability broker scope.
pub fn execute_database_raw_current(
    request_bytes: Vec<u8>,
    cancellation: PluginAbi::PluginCancellationV2
) -> Result<Vec<u8>, DatabaseContracts::DatabaseError> {
    ACTIVE_DATABASE_DISPATCHER.with(|slot| {
        slot.borrow()
            .as_ref()
            .ok_or_else(||
                DatabaseContracts::DatabaseError::new(
                    DatabaseContracts::DatabaseErrorKind::Query,
                    "no active plugin-host database dispatch context"
                )
            )?
            .execute_database_raw(request_bytes, cancellation)
    })
}

impl PluginHostInstance {
    pub(crate) fn database_dispatcher(&self) -> DatabaseDispatcher {
        DatabaseDispatcher::new(&self.inner)
    }

    pub fn new(config: PluginHostConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(PluginManager::new())),
            config,
            database_executor: Mutex::new(None),
            operations: Mutex::new(BTreeMap::new()),
        }
    }

    pub(super) fn executor_selection_for_load(&self) -> Result<PluginExecutorSelection, String> {
        Ok(self.config.executor)
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

#[cfg(test)]
mod host_isolation_tests {
    use super::{
        PluginExecutorSelection,
        PluginHostInstance,
        PluginHostConfig,
        PluginRuntimeConfig,
        ProcessExecutorConfig,
    };
    use std::path::PathBuf;

    fn config(root: &str, executor: PluginExecutorSelection) -> PluginHostConfig {
        PluginHostConfig {
            plugin_root: PathBuf::from(root),
            runtime: PluginRuntimeConfig::default(),
            executor,
            process_executor: ProcessExecutorConfig::default(),
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
