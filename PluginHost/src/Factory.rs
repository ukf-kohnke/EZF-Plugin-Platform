//! Backwards-compatible, process-wide plugin-host facade.
//!
//! New integrations should construct [`PluginHostInstance`] directly. This module intentionally
//! retains the historical module-level functions by routing them through one lazily-created host.
//! The singleton exists only for compatibility; explicit instances never share its registry or
//! database capability.
#![allow(non_snake_case)]

use std::{
    collections::BTreeMap,
    path::{ Path, PathBuf },
    sync::{ Arc, OnceLock },
    time::Duration,
};
use PluginContracts::ApiContract::{
    PluginDatabaseExecuteFn,
    PluginEndpointRequest,
    PluginEndpointResponse,
    PluginEndpointError,
    PluginFreeBufferFn,
    PluginHttpMethod,
    PluginSessionContext,
};

pub use crate::PluginHostInstance::{
    PluginExecutorSelection,
    PluginHostConfig,
    PluginHostInstance,
    PluginLoadFailure,
    PluginLoadPolicy,
    PluginLoadReport,
    PluginManagerError,
    PluginRequirement,
    PluginRuntimeConfig,
    ProcessExecutorConfig,
    SandboxConfig,
    SandboxProfile,
    WorkerEnvironment,
    PluginSessionNotificationFailure,
    PluginSessionNotificationReport,
    PluginExecutorKind,
    PluginHealthSnapshot,
    PluginHealthState,
};

/// Compatibility alias for applications that named the former instance type `Factory::PluginHost`.
pub type PluginHost = PluginHostInstance;

fn shared_instance_cell() -> &'static Arc<PluginHostInstance> {
    static INSTANCE: OnceLock<Arc<PluginHostInstance>> = OnceLock::new();
    INSTANCE.get_or_init(|| Arc::new(PluginHostInstance::new(legacy_host_config())))
}

pub(crate) fn instance() -> &'static PluginHostInstance {
    shared_instance_cell().as_ref()
}

/// Returns the process-wide compatibility host as a shared handle.
/// New application integrations should create and own an explicit PluginHostInstance instead.
#[deprecated(
    note = "construct and inject Arc<PluginHostInstance> at the application composition root"
)]
pub fn shared_instance() -> Arc<PluginHostInstance> {
    Arc::clone(shared_instance_cell())
}

/// Backward-compatible backend/deployment adapter. New integrations should construct
/// PluginHostConfig explicitly and translate environment values at their own composition root.
pub(crate) fn legacy_host_config() -> PluginHostConfig {
    let executor = match std::env::var("EZF_PLUGIN_EXECUTOR") {
        Ok(value) if value.eq_ignore_ascii_case("process") => PluginExecutorSelection::Process,
        _ => PluginExecutorSelection::InProcess,
    };
    let plugin_root = std::env
        ::var_os("EZF_PLUGIN_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let mut root = PathBuf::from(std::env::var_os("EZFBACKEND_HOME").unwrap_or_default());
            root.push("deployment");
            #[cfg(debug_assertions)]
            root.push("debug");
            #[cfg(not(debug_assertions))]
            root.push("release");
            root.push("plugins");
            root
        });
    PluginHostConfig {
        plugin_root,
        runtime: legacy_runtime_config(),
        executor,
        process_executor: legacy_process_executor_config(),
    }
}

fn legacy_runtime_config() -> PluginRuntimeConfig {
    const MAX_TIMEOUT_MS: u64 = 10 * 60 * 1_000;
    const MAX_WORKER_THREADS: usize = 128;
    const MAX_CONCURRENT_CALLS: usize = 4_096;
    const MAX_QUEUE_CAPACITY: usize = 4_096;

    let defaults = PluginRuntimeConfig::default();
    PluginRuntimeConfig {
        call_timeout: env_runtime_duration(
            "EZF_PLUGIN_CALL_TIMEOUT_MS",
            defaults.call_timeout,
            MAX_TIMEOUT_MS
        ),
        unload_timeout: env_runtime_duration(
            "EZF_PLUGIN_UNLOAD_TIMEOUT_MS",
            defaults.unload_timeout,
            MAX_TIMEOUT_MS
        ),
        lifecycle_timeout: env_runtime_duration(
            "EZF_PLUGIN_LIFECYCLE_TIMEOUT_MS",
            defaults.lifecycle_timeout,
            MAX_TIMEOUT_MS
        ),
        worker_threads: env_bounded_usize(
            "EZF_PLUGIN_WORKER_THREADS",
            defaults.worker_threads,
            MAX_WORKER_THREADS
        ),
        max_concurrent_calls: env_bounded_usize(
            "EZF_PLUGIN_MAX_CONCURRENT_CALLS",
            defaults.max_concurrent_calls,
            MAX_CONCURRENT_CALLS
        ),
        queue_capacity: env_bounded_usize(
            "EZF_PLUGIN_QUEUE_CAPACITY",
            defaults.queue_capacity,
            MAX_QUEUE_CAPACITY
        ),
    }
}

fn env_runtime_duration(name: &str, default: Duration, maximum_ms: u64) -> Duration {
    std::env
        ::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .map(|millis| Duration::from_millis(millis.clamp(1, maximum_ms)))
        .unwrap_or(default)
}

fn env_bounded_usize(name: &str, default: usize, maximum: usize) -> usize {
    std::env
        ::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .map(|value| value.clamp(1, maximum))
        .unwrap_or(default)
}

fn legacy_process_executor_config() -> ProcessExecutorConfig {
    let mut config = ProcessExecutorConfig::default();
    config.worker_binary = std::env::var_os("EZF_PLUGIN_WORKER_PATH").map(PathBuf::from);
    config.cancellation_grace = env_duration(
        "EZF_PLUGIN_PROCESS_CANCEL_GRACE_MS",
        Duration::from_millis(500),
        Duration::from_millis(10),
        Duration::from_secs(30)
    );
    config.shutdown_timeout = env_duration(
        "EZF_PLUGIN_PROCESS_SHUTDOWN_TIMEOUT_MS",
        Duration::from_secs(2),
        Duration::from_millis(100),
        Duration::from_secs(15)
    );

    let mut invalid_default_profile = false;

    // The per-plugin mapping takes precedence over the default profile, matching the previous
    // ProcessExecutor behavior. Malformed mappings are encoded as an invalid absolute-path key;
    // ProcessExecutorConfig::validate() rejects that configuration before a worker can start.
    if let Ok(raw) = std::env::var("EZF_PLUGIN_SANDBOX_PROFILES") {
        config.sandbox.require_per_plugin_profile = true;
        match serde_json::from_str::<BTreeMap<String, String>>(&raw) {
            Ok(profiles) => {
                for (path, value) in profiles {
                    match parse_sandbox_profile(&value) {
                        Some(profile) => {
                            config.sandbox.profiles_by_plugin.insert(PathBuf::from(path), profile);
                        }
                        None => {
                            config.sandbox.profiles_by_plugin.insert(
                                PathBuf::new(),
                                SandboxProfile::DevelopmentUnconfined
                            );
                        }
                    }
                }
            }
            Err(_) => {
                config.sandbox.profiles_by_plugin.insert(
                    PathBuf::new(),
                    SandboxProfile::DevelopmentUnconfined
                );
            }
        }
    } else if let Ok(value) = std::env::var("EZF_PLUGIN_SANDBOX_PROFILE") {
        match parse_sandbox_profile(&value) {
            Some(profile) => {
                config.sandbox.default_profile = Some(profile);
            }
            None => {
                // Do not let an invalid profile silently fall back to DevelopmentUnconfined.
                config.sandbox.default_profile = Some(SandboxProfile::DevelopmentUnconfined);
                invalid_default_profile = true;
            }
        }
    }

    config.sandbox.extra_read_paths = env_paths("EZF_PLUGIN_SANDBOX_READ_PATHS");
    config.sandbox.library_paths = env_paths("EZF_PLUGIN_SANDBOX_LIBRARY_PATHS");
    config.sandbox.datasource_egress_policy_acknowledged = std::env
        ::var("EZF_PLUGIN_DATASOURCE_EGRESS_POLICY")
        .ok()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("externally-enforced"));
    config.sandbox.datasource_tcp_ports = std::env
        ::var("EZF_PLUGIN_DATASOURCE_ALLOWED_TCP_PORTS")
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(|item| item.trim().parse::<u16>().unwrap_or(0))
                .collect()
        })
        .unwrap_or_default();
    let explicitly_allow_unconfined = std::env
        ::var("EZF_PLUGIN_ALLOW_UNCONFINED")
        .ok()
        .is_some_and(|value|
            matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes")
        );
    config.sandbox.allow_unconfined = explicitly_allow_unconfined && !invalid_default_profile;

    if let Ok(names) = std::env::var("EZF_PLUGIN_WORKER_ENV_ALLOWLIST") {
        for name in names
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty()) {
            if let Some(value) = std::env::var_os(name) {
                config.worker_environment.variables.insert(name.to_owned(), value);
            }
        }
    }
    config
}

fn env_duration(name: &str, default: Duration, minimum: Duration, maximum: Duration) -> Duration {
    std::env
        ::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(default)
        .clamp(minimum, maximum)
}

fn env_paths(name: &str) -> Vec<PathBuf> {
    std::env
        ::var_os(name)
        .map(|value|
            std::env
                ::split_paths(&value)
                .filter(|path| !path.as_os_str().is_empty())
                .collect()
        )
        .unwrap_or_default()
}

fn parse_sandbox_profile(value: &str) -> Option<SandboxProfile> {
    match value.trim().to_ascii_lowercase().as_str() {
        "domain_plugin" | "domain" => Some(SandboxProfile::DomainPlugin),
        "data_source_plugin" | "datasource" | "data_source" =>
            Some(SandboxProfile::DataSourcePlugin),
        "development_unconfined" | "unconfined" => Some(SandboxProfile::DevelopmentUnconfined),
        _ => None,
    }
}

// Loading and lifecycle
pub fn load_plugin_file(path: &Path) -> Result<i64, PluginManagerError> {
    instance().load_plugin_file_with_config(path, legacy_runtime_config())
}
pub fn load_plugin_file_with_config(
    path: &Path,
    config: PluginRuntimeConfig
) -> Result<i64, PluginManagerError> {
    instance().load_plugin_file_with_config(path, config)
}
pub fn unload_plugin(id: i64) -> Result<(), PluginManagerError> {
    instance().unload_plugin(id)
}
pub fn is_plugin_loaded(plugin_name: &str, plugin_path: &str) -> Result<bool, PluginManagerError> {
    instance().is_plugin_loaded(plugin_name, plugin_path)
}
pub fn load_plugins(plugin_path: &str) -> Result<usize, PluginManagerError> {
    instance()
        .load_plugins_with_policy_and_config(
            plugin_path,
            PluginLoadPolicy::required_all(),
            legacy_runtime_config()
        )
        .map(|report| report.loaded_count())
}
pub fn load_plugins_with_policy(
    plugin_path: &str,
    policy: PluginLoadPolicy
) -> Result<PluginLoadReport, PluginManagerError> {
    instance().load_plugins_with_policy_and_config(plugin_path, policy, legacy_runtime_config())
}
pub fn load_plugins_with_policy_and_config(
    plugin_path: &str,
    policy: PluginLoadPolicy,
    config: PluginRuntimeConfig
) -> Result<PluginLoadReport, PluginManagerError> {
    instance().load_plugins_with_policy_and_config(plugin_path, policy, config)
}
pub fn load_plugin_if_missing(
    plugin_name: &str,
    plugin_path: &str
) -> Result<bool, PluginManagerError> {
    instance().is_plugin_loaded(plugin_name, plugin_path)
}

// Health and readiness
pub fn plugin_health() -> Vec<PluginHealthSnapshot> {
    instance().plugin_health()
}
pub fn plugin_health_by_name(plugin_name: &str) -> Option<PluginHealthSnapshot> {
    instance().plugin_health_by_name(plugin_name)
}
pub fn check_readiness() -> Result<(), String> {
    instance().check_readiness()
}

// Database capability and data-source selection
pub fn register_database_executor(
    executor: PluginDatabaseExecuteFn,
    free: PluginFreeBufferFn
) -> Result<(), String> {
    instance().register_database_executor(executor, free)
}
pub fn check_database_readiness() -> Result<(), String> {
    instance().check_database_readiness()
}
pub fn has_database_plugin() -> bool {
    instance().has_database_plugin()
}
pub fn with_database_plugin<R, F>(func: F) -> Result<R, String>
    where F: FnOnce(&dyn PluginContracts::IPlugin::IPlugin) -> R
{
    instance().with_database_plugin(func)
}
pub fn execute_database_raw(
    request_bytes: Vec<u8>,
    cancellation: PluginAbi::PluginCancellationV2
) -> Result<Vec<u8>, DatabaseContracts::DatabaseError> {
    instance().execute_database_raw(request_bytes, cancellation)
}
pub fn load_database_plugins() -> Result<usize, String> {
    instance().load_database_plugins_with_config(legacy_runtime_config())
}
pub fn load_database_plugins_with_config(config: PluginRuntimeConfig) -> Result<usize, String> {
    instance().load_database_plugins_with_config(config)
}

// Catalog and endpoint routing
pub fn plugin_catalog() -> PluginContracts::ApiContract::PluginCatalog {
    instance().plugin_catalog()
}
pub fn plugin_manifests() -> Vec<PluginContracts::ApiContract::PluginManifest> {
    instance().plugin_manifests()
}
pub fn plugin_names_for_endpoints(endpoints: &[(PluginHttpMethod, &str)]) -> Vec<Option<String>> {
    instance().plugin_names_for_endpoints(endpoints)
}
pub fn loaded_plugin_names() -> Vec<String> {
    instance().loaded_plugin_names()
}
pub fn loaded_plugin_ids() -> Vec<i64> {
    instance().loaded_plugin_ids()
}
pub fn plugin_count() -> usize {
    instance().plugin_count()
}
pub fn has_plugins() -> bool {
    instance().has_plugins()
}
pub fn handle_endpoint(
    request: PluginEndpointRequest
) -> Result<PluginEndpointResponse, PluginEndpointError> {
    instance().handle_endpoint(request)
}
pub fn notify_session_closed(context: &PluginSessionContext) -> PluginSessionNotificationReport {
    instance().notify_session_closed(context)
}

// Deprecated compatibility accessors
pub fn with_plugin_by_name<R, F>(plugin_name: &str, func: F) -> Option<R>
    where F: FnOnce(&dyn PluginContracts::IPlugin::IPlugin) -> R
{
    instance().with_plugin_by_name(plugin_name, func)
}
pub fn with_all_plugins<R, F>(func: F) -> Option<Vec<R>>
    where F: Fn(&dyn PluginContracts::IPlugin::IPlugin) -> R
{
    instance().with_all_plugins(func)
}
