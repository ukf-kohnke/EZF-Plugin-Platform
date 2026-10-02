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
//! - `load_plugin_if_missing` attempts lazy loading from the plugin root path.
//! - Name-based lookup is the canonical way to select a plugin at runtime.
//! - `with_plugin_by_name`, `with_all_plugins`, and `with_database_plugin` provide scoped shared
//!   access to plugin instances via closures. Database execution is selected by manifest capability.
//! - Plugin call limits default to 30s call timeout, 10s unload timeout, and 64 concurrent calls.
//!   They can be overridden with `EZF_PLUGIN_CALL_TIMEOUT_MS`, `EZF_PLUGIN_UNLOAD_TIMEOUT_MS`,
//!   and `EZF_PLUGIN_MAX_CONCURRENT_CALLS`, or supplied explicitly to the `*_with_config` load APIs.
//!
//! # Return conventions
//!
//! - `is_plugin_loaded` / `load_plugin_if_missing`: `Ok(true)` when plugin is
//!   available after optional lazy loading.
//! - `with_*` helpers: `None` when no matching plugin (or no plugins) is present.
//! - bulk loading returns a [`Factory::PluginLoadReport`] when policy-aware loading is used;
//!   the compatibility [`Factory::load_plugins`] API continues to return a loaded count.
#![allow(non_snake_case)]
mod PluginManager;
pub mod Factory {
    use super::PluginManager::{ PluginManager, PluginProxy };
    pub use super::PluginManager::{ PluginHealthSnapshot, PluginHealthState, PluginRuntimeConfig };
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
        collections::{ BTreeMap, BTreeSet },
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
        // Reserve the library path for the whole unload operation. This prevents a concurrent
        // reload/load of the same path from racing the final `Library` drop after quiescing.
        let canonical_path = {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            manager.plugins
                .get(&id)
                .map(|item| item.path.clone())
                .ok_or(PluginManagerError::PluginNotFound { id })?
        };
        let _operation_guard = begin_plugin_operation(&canonical_path)?;

        // Re-check membership after acquiring the operation guard because another lifecycle
        // operation may have completed between the initial lookup and reservation.
        let mut item = {
            let singleton = instance();
            let mut manager = singleton.inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            manager.detach_plugin(id)?
        };

        let started = std::time::Instant::now();
        if item.unload() {
            let elapsed = started.elapsed();
            Common::Logging::Logger::Log::Dump(
                "{}",
                Common::Logging::LogConstants::LOGGERDEF,
                log::LevelFilter::Info,
                &[&format!("Plugin unloaded successfully: id={} after {:?}", id, elapsed)]
            );
            drop(item);
            Ok(())
        } else {
            let singleton = instance();
            let mut manager = singleton.inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            manager.restore_plugin(id, item);
            Common::Logging::Logger::Log::Dump(
                "{}",
                Common::Logging::LogConstants::LOGGERDEF,
                log::LevelFilter::Error,
                &[&format!("Plugin unload failed or timed out: id={}", id)]
            );
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

    static PLUGIN_OPERATIONS_IN_PROGRESS: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();

    fn plugin_operations_in_progress() -> &'static Mutex<BTreeSet<PathBuf>> {
        PLUGIN_OPERATIONS_IN_PROGRESS.get_or_init(|| Mutex::new(BTreeSet::new()))
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

    fn begin_plugin_operation(path: &Path) -> Result<PluginOperationGuard, PluginManagerError> {
        let canonical_path = PluginManager::normalize_path(path);
        let mut paths = plugin_operations_in_progress()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !paths.insert(canonical_path.clone()) {
            return Err(PluginManagerError::PluginLoadInProgress { path: canonical_path });
        }
        Ok(PluginOperationGuard { path: canonical_path })
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

        let _operation_guard = begin_plugin_operation(&canonical_path)?;

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
            Common::Logging::Logger::Log::Dump(
                "{}",
                Common::Logging::LogConstants::LOGGERDEF,
                log::LevelFilter::Info,
                &[&format!(
                    "Plugin loaded successfully: '{}' (id={}) from '{}'",
                    plugin_manifest.name,
                    id,
                    canonical_path.display()
                )]
            );
        }
        result
    }

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

            let path_requirement = policy.requirement_for_path(&canonical_path);
            let _operation_guard = match begin_plugin_operation(&canonical_path) {
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
                failure
                    .plugin_name
                    .as_deref()
                    .is_some_and(|name| normalize_plugin_name(name) == *configured_name)
                    || failure
                        .path
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
            Common::Logging::Logger::Log::Dump(
                "{}",
                Common::Logging::LogConstants::LOGGERDEF,
                log::LevelFilter::Warn,
                &[
                    &format!("No plugin libraries were found in the configured directory '{}'.", plugin_path),
                ]
            );
        }

        for failure in &report.failures {
            let level = match failure.requirement {
                PluginRequirement::Required => log::LevelFilter::Error,
                PluginRequirement::Optional => log::LevelFilter::Warn,
            };
            Common::Logging::Logger::Log::Dump(
                "{}",
                Common::Logging::LogConstants::LOGGERDEF,
                level,
                &[
                    &format!(
                        "{} plugin '{}' at '{}' could not be loaded: {}",
                        match failure.requirement {
                            PluginRequirement::Required => "Required",
                            PluginRequirement::Optional => "Optional",
                        },
                        failure.plugin_name.as_deref().unwrap_or("unknown"),
                        failure.path.display(),
                        failure.error
                    ),
                ]
            );
        }

        Common::Logging::Logger::Log::Dump(
            "{}",
            Common::Logging::LogConstants::LOGGERDEF,
            if report.has_required_failures() {
                log::LevelFilter::Error
            } else {
                log::LevelFilter::Info
            },
            &[&report.summary()]
        );

        if report.has_required_failures() {
            return Err(PluginManagerError::PluginLoadPolicyViolation { report });
        }

        Ok(report)
    }

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
            .filter_map(|id| manager.plugins.get(&id).map(|item| item.health()))
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
                item.manifest().name.eq_ignore_ascii_case(plugin_name).then(|| item.health())
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
        let database_root = PathBuf::from(getPluginRootDir()).join("database");
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
            loaded_count += load_plugins(&path).map_err(|error|
                format!("failed to load database plugins from '{}': {error}", path)
            )?;
        }

        Ok(loaded_count)
    }

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
            let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let id = manager
                .plugin_ids()
                .into_iter()
                .find(|id|
                    manager
                    .get_manifest(*id)
                    .is_some_and(|manifest| manifest.name.eq_ignore_ascii_case(plugin_name))
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

        runtime.handle_endpoint(request)
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
