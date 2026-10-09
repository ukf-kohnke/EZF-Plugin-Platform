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
//! - Expose bounded runtime execution, health snapshots, and a readiness gate.
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
//! The global mutex protects only plugin-manager state (registration, lookup, catalog and lifecycle
//! bookkeeping). Each loaded plugin has its own read/write runtime gate: normal ABI calls take a
//! shared read lock and can execute concurrently, while unload marks the runtime as quiescing,
//! rejects new calls, and takes the exclusive write lock to drain all in-flight calls before
//! lifecycle callbacks and instance destruction. A per-library lifecycle-operation guard also
//! prevents load/unload races for the same dynamic library path.
//!
//! # Behavior notes
//!
//! - `load_plugin_if_missing` is a deprecated compatibility API; prefer explicit catalog/health lookup.
//! - REST endpoint lookup is catalog-based: callers identify an endpoint by its HTTP contract,
//!   not by a concrete plugin implementation name.
//! - `with_plugin_by_name` and `with_all_plugins` are deprecated compatibility APIs; prefer the
//!   catalog/endpoint APIs. `with_database_plugin` remains the explicit capability boundary.
//! - Plugin call limits default to 30s call timeout, 10s unload timeout, 8 worker threads,
//!   32 admitted calls, and 24 queued jobs. The three capacity limits can be overridden with
//!   `EZF_PLUGIN_WORKER_THREADS`, `EZF_PLUGIN_MAX_CONCURRENT_CALLS`, and
//!   `EZF_PLUGIN_QUEUE_CAPACITY`, or supplied explicitly to the `*_with_config` load APIs.
//! - ABI V2 uses cooperative cancellation for plugin calls. A caller-side timeout sets the
//!   per-operation cancellation signal; plugin code must observe it at safe interruption points
//!   and return. The host never terminates a plugin thread or invalidates plugin memory.
//! - On unload, every admitted call receives the same cancellation request and the runtime waits
//!   for those calls to drain before `on_unload` and `destroy` are started. Lifecycle callbacks
//!   themselves also receive a cooperative cancellation token. A lifecycle timeout leaves the
//!   runtime registered until the callback actually returns, so a retry can safely continue.
//!
//! # Return conventions
//!
//! - `is_plugin_loaded`: `Ok(true)` when a plugin is available.
//! - `with_*` helpers: `None` when no matching plugin (or no plugins) is present.
//! - bulk loading returns a [`Factory::PluginLoadReport`] from the policy-aware API.
#![allow(non_snake_case)]

#[cfg(target_os = "linux")]
const LOGGERDEF: &str = "unix";
#[cfg(target_os = "macos")]
const LOGGERDEF: &str = "macos";
#[cfg(target_os = "windows")]
const LOGGERDEF: &str = "windows";

fn emit_log(level: log::LevelFilter, message: &str) {
    match level {
        log::LevelFilter::Error => log::error!(target: LOGGERDEF, "{}", message),
        log::LevelFilter::Warn => log::warn!(target: LOGGERDEF, "{}", message),
        log::LevelFilter::Info => log::info!(target: LOGGERDEF, "{}", message),
        log::LevelFilter::Debug => log::debug!(target: LOGGERDEF, "{}", message),
        log::LevelFilter::Trace => log::trace!(target: LOGGERDEF, "{}", message),
        log::LevelFilter::Off => {}
    }
}
mod PluginManager;
pub mod Factory {
    use crate::emit_log;

    use super::PluginManager::{ PluginManager, PluginProxy };
    pub use super::PluginManager::{ PluginHealthSnapshot, PluginHealthState, PluginRuntimeConfig };
    use PluginContracts::{
        IPlugin::IPlugin,
        ApiContract::{
            PluginCancellation,
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
    use ApplicationConfig::plugin_root_dir;
    use glob::PatternError;
    use std::{
        collections::BTreeMap,
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

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum PluginRequirement {
        Required,
        Optional,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct PluginLoadFailure {
        pub path: PathBuf,
        pub plugin_name: Option<String>,
        pub requirement: PluginRequirement,
        pub error: String,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct PluginLoadReport {
        pub requested_path: PathBuf,
        pub discovered: Vec<PathBuf>,
        pub loaded: Vec<String>,
        pub skipped_already_loaded: Vec<PathBuf>,
        pub failures: Vec<PluginLoadFailure>,
    }

    impl PluginLoadReport {
        pub fn required_failures(&self) -> Vec<&PluginLoadFailure> {
            self.failures
                .iter()
                .filter(|failure| failure.requirement == PluginRequirement::Required)
                .collect()
        }

        pub fn has_required_failures(&self) -> bool {
            self.failures.iter().any(|failure| failure.requirement == PluginRequirement::Required)
        }

        pub fn is_success(&self) -> bool {
            !self.has_required_failures()
        }

        pub fn loaded_count(&self) -> usize {
            self.loaded.len()
        }

        pub fn failure_count(&self) -> usize {
            self.failures.len()
        }

        fn summary(&self) -> String {
            format!(
                "plugin load report for '{}': discovered={}, loaded={}, skipped={}, failures={}, required_failures={}",
                self.requested_path.display(),
                self.discovered.len(),
                self.loaded.len(),
                self.skipped_already_loaded.len(),
                self.failures.len(),
                self.required_failures().len()
            )
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct PluginLoadPolicy {
        default_requirement: PluginRequirement,
        overrides: BTreeMap<String, PluginRequirement>,
    }

    impl PluginLoadPolicy {
        /// Treat every discovered plugin as required unless explicitly overridden.
        pub fn required_all() -> Self {
            Self {
                default_requirement: PluginRequirement::Required,
                overrides: BTreeMap::new(),
            }
        }

        /// Treat every discovered plugin as optional unless explicitly required.
        pub fn optional_all() -> Self {
            Self {
                default_requirement: PluginRequirement::Optional,
                overrides: BTreeMap::new(),
            }
        }

        pub fn require(mut self, plugin_name: impl Into<String>) -> Self {
            self.overrides.insert(
                normalize_plugin_name(&plugin_name.into()),
                PluginRequirement::Required
            );
            self
        }

        pub fn optional(mut self, plugin_name: impl Into<String>) -> Self {
            self.overrides.insert(
                normalize_plugin_name(&plugin_name.into()),
                PluginRequirement::Optional
            );
            self
        }

        pub fn requirement_for_name(&self, plugin_name: &str) -> PluginRequirement {
            self.overrides
                .get(&normalize_plugin_name(plugin_name))
                .copied()
                .unwrap_or(self.default_requirement)
        }

        pub fn requirement_for_path(&self, path: &Path) -> PluginRequirement {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .map(|stem| self.requirement_for_name(stem))
                .unwrap_or(self.default_requirement)
        }
    }

    fn normalize_plugin_name(name: &str) -> String {
        name.trim().to_ascii_lowercase()
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
        PluginLoadPolicyViolation {
            report: PluginLoadReport,
        },
        PluginLoadInProgress {
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
                PluginManagerError::PluginLoadPolicyViolation { report } =>
                    write!(f, "{}", report.summary()),
                PluginManagerError::PluginLoadInProgress { path } =>
                    write!(f, "Plugin at {:?} is already being loaded", path),
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
    pub fn unload_plugin(id: i64) -> Result<(), PluginManagerError> {
        // Keep the plugin registered for the complete unload operation. `PluginRuntime::unload`
        // transitions its health to `Stopping` before waiting for in-flight calls to drain.
        // Keeping the registry entry visible makes that transient state observable through the
        // public readiness/health APIs and prevents an active unload from looking like an empty,
        // ready host. The operation guard still prevents a concurrent load/reload of the same
        // canonical library path.
        let canonical_path = {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            manager.plugins
                .get(&id)
                .map(|item| item.path.clone())
                .ok_or(PluginManagerError::PluginNotFound { id })?
        };
        let _operation_guard = begin_plugin_operation(
            &canonical_path,
            PluginOperationKind::Unloading
        )?;

        // Re-check membership after acquiring the operation guard because another lifecycle
        // operation may have completed between the initial lookup and reservation. Only the
        // runtime Arc is borrowed here; the registry entry itself stays in place until unload
        // succeeds, so health snapshots can expose `Stopping`/failure states during the operation.
        let runtime = {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            manager.get_runtime(id).ok_or(PluginManagerError::PluginNotFound { id })?
        };

        let started = std::time::Instant::now();
        if runtime.unload() {
            let elapsed = started.elapsed();
            let removed_item = {
                let singleton = instance();
                let mut manager = singleton.inner
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                manager.detach_plugin(id)?
            };

            emit_log(
                log::LevelFilter::Info,
                &format!("Plugin unloaded successfully: id={} after {:?}", id, elapsed)
            );
            drop(removed_item);
            Ok(())
        } else {
            emit_log(
                log::LevelFilter::Error,
                &format!("Plugin unload failed or timed out: id={}", id)
            );
            Err(PluginManagerError::PluginUnloadFailed { id })
        }
    }

    pub fn is_plugin_loaded(
        plugin_name: &str,
        plugin_path: &str
    ) -> Result<bool, PluginManagerError> {
        if find_id_by_name(plugin_name).is_some() {
            return Ok(true);
        }

        let report = load_plugins_with_policy(plugin_path, PluginLoadPolicy::optional_all())?;
        Ok(report.loaded_count() > 0 && find_id_by_name(plugin_name).is_some())
    }

    pub(crate) fn find_id_by_name(plugin_name: &str) -> Option<i64> {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        manager
            .plugin_ids()
            .into_iter()
            .find(|id|
                manager
                    .get_manifest(*id)
                    .is_some_and(|manifest| manifest.name.eq_ignore_ascii_case(plugin_name))
            )
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum PluginOperationKind {
        Loading,
        Unloading,
    }

    static PLUGIN_OPERATIONS_IN_PROGRESS: OnceLock<
        Mutex<BTreeMap<PathBuf, PluginOperationKind>>
    > = OnceLock::new();

    fn plugin_operations_in_progress() -> &'static Mutex<BTreeMap<PathBuf, PluginOperationKind>> {
        PLUGIN_OPERATIONS_IN_PROGRESS.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    struct PluginOperationGuard {
        path: PathBuf,
    }

    impl Drop for PluginOperationGuard {
        fn drop(&mut self) {
            let mut paths = plugin_operations_in_progress()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            paths.remove(&self.path);
        }
    }

    fn begin_plugin_operation(
        path: &Path,
        kind: PluginOperationKind
    ) -> Result<PluginOperationGuard, PluginManagerError> {
        let canonical_path = PluginManager::normalize_path(path);
        let mut paths = plugin_operations_in_progress()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if paths.contains_key(&canonical_path) {
            return Err(PluginManagerError::PluginLoadInProgress { path: canonical_path });
        }
        paths.insert(canonical_path.clone(), kind);
        Ok(PluginOperationGuard { path: canonical_path })
    }

    fn plugin_operation_kind(path: &Path) -> Option<PluginOperationKind> {
        let canonical_path = PluginManager::normalize_path(path);
        plugin_operations_in_progress()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&canonical_path)
            .copied()
    }

    pub fn load_plugin_file(path: &Path) -> Result<i64, PluginManagerError> {
        load_plugin_file_with_config(path, PluginRuntimeConfig::from_environment())
    }

    pub fn load_plugin_file_with_config(
        path: &Path,
        config: PluginRuntimeConfig
    ) -> Result<i64, PluginManagerError> {
        load_plugin_file_inner(path, config)
    }

    fn load_plugin_file_inner(
        path: &Path,
        config: PluginRuntimeConfig
    ) -> Result<i64, PluginManagerError> {
        let canonical_path = PluginManager::normalize_path(path);

        // Reserve the canonical library path before inspecting the registry. This closes the
        // unload/load race where an unload could remove the registry entry between the initial
        // duplicate check and acquisition of the lifecycle operation guard.
        let _operation_guard = begin_plugin_operation(
            &canonical_path,
            PluginOperationKind::Loading
        )?;

        {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(existing_id) = manager.paths.get(&canonical_path).copied() {
                return Err(PluginManagerError::PluginAlreadyLoaded {
                    path: canonical_path,
                    id: existing_id,
                });
            }
        }

        let loaded = PluginManager::load_plugin(&canonical_path, config)?;
        let plugin_manifest = loaded.manifest().clone();

        let result = {
            let singleton = instance();
            let mut manager = singleton.inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            if let Some(existing_id) = manager.paths.get(&canonical_path).copied() {
                Err(PluginManagerError::PluginAlreadyLoaded {
                    path: canonical_path.clone(),
                    id: existing_id,
                })
            } else {
                match manager.catalog_with_manifest(&plugin_manifest) {
                    Ok(catalog) =>
                        Ok(manager.insert_loaded_plugin(&canonical_path, loaded, catalog)),
                    Err(error) => Err(error),
                }
            }
        };

        if let Ok(id) = result.as_ref() {
            emit_log(
                log::LevelFilter::Info,
                &format!(
                    "Plugin loaded successfully: '{}' (id={}) from '{}'",
                    plugin_manifest.name,
                    id,
                    canonical_path.display()
                )
            );
        }
        result
    }

    #[deprecated(
        note = "use load_plugins_with_policy or load_plugins_with_policy_and_config; the legacy required-all policy is implicit and provides no load report"
    )]
    pub fn load_plugins(plugin_path: &str) -> Result<usize, PluginManagerError> {
        let report = load_plugins_with_policy_and_config(
            plugin_path,
            PluginLoadPolicy::required_all(),
            PluginRuntimeConfig::from_environment()
        )?;
        Ok(report.loaded_count())
    }

    pub fn load_plugins_with_policy(
        plugin_path: &str,
        policy: PluginLoadPolicy
    ) -> Result<PluginLoadReport, PluginManagerError> {
        load_plugins_with_policy_and_config(
            plugin_path,
            policy,
            PluginRuntimeConfig::from_environment()
        )
    }

    pub fn load_plugins_with_policy_and_config(
        plugin_path: &str,
        policy: PluginLoadPolicy,
        config: PluginRuntimeConfig
    ) -> Result<PluginLoadReport, PluginManagerError> {
        let entries = PluginManager::discover_plugin_paths(plugin_path)?;
        let mut report = PluginLoadReport {
            requested_path: PathBuf::from(plugin_path),
            ..PluginLoadReport::default()
        };

        for entry in entries {
            let canonical_path = match entry {
                Ok(path) => path,
                Err(error) => {
                    report.failures.push(PluginLoadFailure {
                        path: PathBuf::from(plugin_path),
                        plugin_name: None,
                        requirement: policy.default_requirement,
                        error,
                    });
                    continue;
                }
            };
            report.discovered.push(canonical_path.clone());

            let path_requirement = policy.requirement_for_path(&canonical_path);
            // Reserve the path before checking the registry for the same reason as the direct
            // file-load path: a concurrent unload must not be able to create a reload window.
            let _operation_guard = match
                begin_plugin_operation(&canonical_path, PluginOperationKind::Loading)
            {
                Ok(guard) => guard,
                Err(error) => {
                    report.failures.push(PluginLoadFailure {
                        path: canonical_path,
                        plugin_name: None,
                        requirement: path_requirement,
                        error: error.to_string(),
                    });
                    continue;
                }
            };

            {
                let singleton = instance();
                let manager = singleton.inner
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if manager.paths.contains_key(&canonical_path) {
                    report.skipped_already_loaded.push(canonical_path);
                    continue;
                }
            }

            let loaded = match PluginManager::load_plugin(&canonical_path, config) {
                Ok(loaded) => loaded,
                Err(error) => {
                    let error_text = error.to_string();
                    report.failures.push(PluginLoadFailure {
                        path: canonical_path,
                        plugin_name: None,
                        requirement: path_requirement,
                        error: error_text,
                    });
                    continue;
                }
            };

            let manifest = loaded.manifest().clone();
            let manifest_requirement = policy.requirement_for_name(&manifest.name);
            let requirement = if
                path_requirement == PluginRequirement::Required ||
                manifest_requirement == PluginRequirement::Required
            {
                PluginRequirement::Required
            } else {
                PluginRequirement::Optional
            };
            let plugin_name = manifest.name.clone();

            let registration = {
                let singleton = instance();
                let mut manager = singleton.inner
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());

                if let Some(existing_id) = manager.paths.get(&canonical_path).copied() {
                    Err(PluginManagerError::PluginAlreadyLoaded {
                        path: canonical_path.clone(),
                        id: existing_id,
                    })
                } else {
                    match manager.catalog_with_manifest(&manifest) {
                        Ok(catalog) =>
                            Ok(manager.insert_loaded_plugin(&canonical_path, loaded, catalog)),
                        Err(error) => Err(error),
                    }
                }
            };

            match registration {
                Ok(_) => report.loaded.push(plugin_name),
                Err(error) =>
                    report.failures.push(PluginLoadFailure {
                        path: canonical_path,
                        plugin_name: Some(plugin_name),
                        requirement,
                        error: error.to_string(),
                    }),
            }
        }

        let loaded_names = loaded_plugin_names();
        for (configured_name, requirement) in &policy.overrides {
            if *requirement != PluginRequirement::Required {
                continue;
            }

            let present =
                loaded_names
                    .iter()
                    .any(|loaded_name| normalize_plugin_name(loaded_name) == *configured_name) ||
                report.loaded
                    .iter()
                    .any(|loaded_name| normalize_plugin_name(loaded_name) == *configured_name);

            let matching_failure_index = report.failures.iter().position(|failure| {
                failure.plugin_name
                    .as_deref()
                    .is_some_and(|name| normalize_plugin_name(name) == *configured_name) ||
                    failure.path
                        .file_stem()
                        .and_then(|stem| stem.to_str())
                        .is_some_and(|stem| normalize_plugin_name(stem) == *configured_name)
            });

            // A required plugin may fail before its manifest can be read (for example,
            // when the DLL is not a valid dynamic library). In that case we still want
            // the load report to identify the failed required plugin when the configured
            // name unambiguously matches the failing file, without creating a duplicate
            // synthetic failure for the same root cause.
            if let Some(index) = matching_failure_index {
                if report.failures[index].plugin_name.is_none() {
                    report.failures[index].plugin_name = Some(configured_name.clone());
                }
            }

            let already_reported_for_name = matching_failure_index.is_some();

            if !present && !already_reported_for_name {
                report.failures.push(PluginLoadFailure {
                    path: PathBuf::from(plugin_path),
                    plugin_name: Some(configured_name.clone()),
                    requirement: PluginRequirement::Required,
                    error: format!("required plugin '{}' was not discovered or loaded", configured_name),
                });
            }
        }

        // A required plugin load failure already represents the missing/unusable plugin.
        // Do not add a second synthetic "not discovered or loaded" failure for the same
        // configured name; that would double-count one root cause and make the load report
        // misleading.
        if report.discovered.is_empty() && report.failures.is_empty() {
            emit_log(
                log::LevelFilter::Warn,
                &format!("No plugin libraries were found in the configured directory '{}'.", plugin_path)
            );
        }

        for failure in &report.failures {
            let level = match failure.requirement {
                PluginRequirement::Required => log::LevelFilter::Error,
                PluginRequirement::Optional => log::LevelFilter::Warn,
            };
            emit_log(
                level,
                &format!(
                    "{} plugin '{}' at '{}' could not be loaded: {}",
                    match failure.requirement {
                        PluginRequirement::Required => "Required",
                        PluginRequirement::Optional => "Optional",
                    },
                    failure.plugin_name.as_deref().unwrap_or("unknown"),
                    failure.path.display(),
                    failure.error
                )
            );
        }

        emit_log(
            if report.has_required_failures() {
                log::LevelFilter::Error
            } else {
                log::LevelFilter::Info
            },
            &report.summary()
        );

        if report.has_required_failures() {
            return Err(PluginManagerError::PluginLoadPolicyViolation { report });
        }

        Ok(report)
    }

    #[deprecated(
        note = "use is_plugin_loaded for lookup; explicit loading should use load_plugin_file or load_plugins_with_policy"
    )]
    pub fn load_plugin_if_missing(
        plugin_name: &str,
        plugin_path: &str
    ) -> Result<bool, PluginManagerError> {
        is_plugin_loaded(plugin_name, plugin_path)
    }

    /// Returns health/telemetry snapshots for all loaded plugins in deterministic plugin-id order.
    pub fn plugin_health() -> Vec<PluginHealthSnapshot> {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        manager
            .plugin_ids()
            .into_iter()
            .filter_map(|id| {
                let item = manager.plugins.get(&id)?;
                let mut health = item.health();
                if plugin_operation_kind(&item.path) == Some(PluginOperationKind::Unloading) {
                    health.state = PluginHealthState::Stopping;
                }
                Some(health)
            })
            .collect()
    }

    /// Returns the health snapshot for a specific plugin using case-insensitive name matching.
    pub fn plugin_health_by_name(plugin_name: &str) -> Option<PluginHealthSnapshot> {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        manager
            .plugin_ids()
            .into_iter()
            .find_map(|id| {
                let item = manager.plugins.get(&id)?;
                if !item.manifest().name.eq_ignore_ascii_case(plugin_name) {
                    return None;
                }

                let mut health = item.health();
                if plugin_operation_kind(&item.path) == Some(PluginOperationKind::Unloading) {
                    health.state = PluginHealthState::Stopping;
                }
                Some(health)
            })
    }

    /// Returns an error unless every loaded plugin is currently ready. This can be used as a
    /// startup/readiness gate after plugin discovery and before serving traffic.
    pub fn check_readiness() -> Result<(), String> {
        let health = plugin_health();
        let degraded = health
            .iter()
            .filter(|snapshot| snapshot.state != PluginHealthState::Ready)
            .map(|snapshot| format!("{}={:?}", snapshot.name, snapshot.state))
            .collect::<Vec<_>>();
        if degraded.is_empty() {
            Ok(())
        } else {
            Err(format!("plugin readiness check failed: {}", degraded.join(", ")))
        }
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
        where F: FnOnce(&dyn IPlugin) -> R
    {
        let (runtimes, names) = {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

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
                let proxy = PluginProxy::new(runtimes[0].clone());
                Ok(func(&proxy))
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
        let database_root = PathBuf::from(plugin_root_dir()).join("database");
        let entries = fs
            ::read_dir(&database_root)
            .map_err(|error|
                format!(
                    "failed to read database plugin root '{}': {error}",
                    database_root.display()
                )
            )?;

        let mut directories = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error|
                format!(
                    "failed to inspect database plugin root '{}': {error}",
                    database_root.display()
                )
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
            let report = load_plugins_with_policy(&path, PluginLoadPolicy::optional_all()).map_err(
                |error| format!("failed to load database plugins from '{}': {error}", path)
            )?;
            loaded_count += report.loaded_count();
        }

        Ok(loaded_count)
    }

    #[deprecated(
        note = "prefer catalog/endpoint dispatch; concrete plugin-name access is a compatibility API"
    )]
    pub fn with_plugin_by_name<R, F>(plugin_name: &str, func: F) -> Option<R>
        where F: FnOnce(&dyn IPlugin) -> R
    {
        if plugin_name.is_empty() {
            return None;
        }

        let runtime = {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let id = manager
                .plugin_ids()
                .into_iter()
                .find(|id|
                    manager
                        .get_manifest(*id)
                        .is_some_and(|manifest| manifest.name.eq_ignore_ascii_case(plugin_name))
                )?;
            manager.get_runtime(id)?
        };

        {
            let proxy = PluginProxy::new(runtime);
            Some(func(&proxy))
        }
    }

    pub fn plugin_catalog() -> PluginCatalog {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        let catalog = manager.catalog();
        catalog
    }

    #[deprecated(
        note = "prefer catalog/health queries and explicit capability APIs; bulk raw plugin iteration is a compatibility API"
    )]
    pub fn with_all_plugins<R, F>(func: F) -> Option<Vec<R>> where F: Fn(&dyn IPlugin) -> R {
        let runtimes = {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
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
                let proxy = PluginProxy::new(runtime);
                Some(func(&proxy))
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
                    let proxy = PluginProxy::new(runtime.clone());
                    proxy.on_session_closed(context, &PluginCancellation::uncancelled())
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
        let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        manager
            .plugin_ids()
            .into_iter()
            .filter_map(|id| manager.get_manifest(id).map(|manifest| manifest.name.to_string()))
            .collect()
    }

    pub fn loaded_plugin_ids() -> Vec<i64> {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        manager.plugin_ids()
    }

    pub fn plugin_count() -> usize {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        manager.plugin_count()
    }

    pub fn has_plugins() -> bool {
        plugin_count() > 0
    }

    /// Executes a plugin REST endpoint without requiring the caller to know the concrete plugin name.
    ///
    /// Endpoint ownership is resolved from the runtime catalog using the HTTP method and path.
    /// This prevents application code from coupling itself to implementation-specific plugin
    /// names such as `ProcessTracker` and also avoids fragile name-derivation heuristics from REST
    /// function names.
    pub fn handle_endpoint(
        request: PluginEndpointRequest
    ) -> Result<PluginEndpointResponse, PluginEndpointError> {
        let endpoint_path = request_endpoint_path(&request.base_path, &request.raw_path);
        let plugin_name = resolve_endpoint_plugin_name(
            request.method,
            &endpoint_path,
            &request.endpoint_id
        )?;

        let runtime = {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let id = manager
                .plugin_ids()
                .into_iter()
                .find(|id|
                    manager
                        .get_manifest(*id)
                        .is_some_and(|manifest| manifest.name.eq_ignore_ascii_case(&plugin_name))
                )
                .ok_or_else(||
                    PluginEndpointError::not_found(
                        "plugin_not_found",
                        "The plugin implementing this endpoint is not available",
                        &format!(
                            "No loaded plugin implements {} {}",
                            request.method,
                            request.raw_path
                        )
                    )
                )?;
            manager
                .get_runtime(id)
                .ok_or_else(||
                    PluginEndpointError::not_found(
                        "plugin_runtime_not_found",
                        "The plugin runtime implementing this endpoint is not available",
                        &format!("No runtime is registered for plugin '{}'", plugin_name)
                    )
                )?
        };

        runtime.handle_endpoint(request)
    }

    fn request_endpoint_path(base_path: &str, raw_path: &str) -> String {
        let base = base_path.trim();
        let raw = if raw_path.trim().is_empty() { "/" } else { raw_path.trim() };

        if base.is_empty() || base == "/" {
            return raw.to_owned();
        }

        let base = format!("/{}", base.trim_matches('/'));
        if raw == base {
            return "/".to_owned();
        }

        raw.strip_prefix(&format!("{base}/"))
            .map(|path| format!("/{path}"))
            .unwrap_or_else(|| raw.to_owned())
    }

    fn resolve_endpoint_plugin_name(
        method: PluginContracts::ApiContract::PluginHttpMethod,
        path: &str,
        endpoint_id: &str
    ) -> Result<String, PluginEndpointError> {
        let resolve_from_catalog = || {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            manager
                .catalog()
                .endpoint_binding(method, path)
                .map(|binding| {
                    if !binding.endpoint.id.eq_ignore_ascii_case(endpoint_id) {
                        return Err(
                            PluginEndpointError::internal(
                                "plugin_endpoint_contract_mismatch",
                                "The requested endpoint contract does not match the registered REST endpoint",
                                format!(
                                    "HTTP endpoint {} {} is registered as endpoint id '{}', not '{}'",
                                    method,
                                    path,
                                    binding.endpoint.id,
                                    endpoint_id
                                )
                            )
                        );
                    }
                    Ok(binding.plugin_name.clone())
                })
        };

        if let Some(result) = resolve_from_catalog() {
            return result;
        }

        // Preserve the previous lazy-load behavior without reintroducing plugin-name coupling:
        // discover all plugin libraries, then resolve the requested endpoint from the resulting
        // catalog. Optional discovery failures do not prevent other plugins from loading.
        load_plugins_with_policy(&plugin_root_dir(), PluginLoadPolicy::optional_all()).map_err(
            |error|
                PluginEndpointError::internal(
                    "plugin_discovery_failed",
                    "Plugin discovery failed while resolving the endpoint",
                    error.to_string()
                )
        )?;

        match resolve_from_catalog() {
            Some(result) => result,
            None =>
                Err(
                    PluginEndpointError::not_found(
                        "plugin_endpoint_not_found",
                        "No loaded plugin implements the requested endpoint",
                        &format!("No plugin implements {} {}", method, path)
                    )
                ),
        }
    }
}

#[cfg(test)]
mod load_policy_tests {
    use super::Factory::{ PluginLoadPolicy, PluginRequirement };
    use std::path::Path;

    #[test]
    fn explicit_plugin_requirement_overrides_default_by_name_and_path() {
        let policy = PluginLoadPolicy::optional_all().require("ProcessTracker");
        assert_eq!(policy.requirement_for_name("processtracker"), PluginRequirement::Required);
        assert_eq!(
            policy.requirement_for_path(Path::new("/plugins/ProcessTracker.so")),
            PluginRequirement::Required
        );
        assert_eq!(
            policy.requirement_for_path(Path::new("/plugins/VariantFilter.so")),
            PluginRequirement::Optional
        );
    }
}

#[cfg(test)]
mod load_report_tests {
    use super::Factory::{ PluginLoadFailure, PluginLoadReport, PluginRequirement };
    use std::path::PathBuf;

    #[test]
    fn report_distinguishes_required_and_optional_failures() {
        let mut report = PluginLoadReport {
            requested_path: PathBuf::from("/plugins"),
            ..PluginLoadReport::default()
        };
        report.failures.push(PluginLoadFailure {
            path: PathBuf::from("/plugins/optional.so"),
            plugin_name: Some("Optional".to_owned()),
            requirement: PluginRequirement::Optional,
            error: "load failed".to_owned(),
        });
        assert!(report.is_success());

        report.failures.push(PluginLoadFailure {
            path: PathBuf::from("/plugins/required.so"),
            plugin_name: Some("Required".to_owned()),
            requirement: PluginRequirement::Required,
            error: "load failed".to_owned(),
        });
        assert!(!report.is_success());
        assert_eq!(report.required_failures().len(), 1);
    }
}
