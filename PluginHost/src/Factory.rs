//! Backwards-compatible, process-wide plugin-host facade.
//!
//! New integrations should construct [`PluginHostInstance`] directly. This module intentionally
//! retains the historical module-level functions by routing them through one lazily-created host.
//! The singleton exists only for compatibility; explicit instances never share its registry or
//! database capability.
#![allow(non_snake_case)]

use std::sync::OnceLock;
use std::path::Path;
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
    PluginSessionNotificationFailure,
    PluginSessionNotificationReport,
    PluginExecutorKind,
    PluginHealthSnapshot,
    PluginHealthState,
};

/// Compatibility alias for applications that named the former instance type `Factory::PluginHost`.
pub type PluginHost = PluginHostInstance;

pub(crate) fn instance() -> &'static PluginHostInstance {
    static INSTANCE: OnceLock<PluginHostInstance> = OnceLock::new();
    INSTANCE.get_or_init(|| PluginHostInstance::_new_legacy(PluginHostConfig::from_environment()))
}

// Loading and lifecycle
pub fn load_plugin_file(path: &Path) -> Result<i64, PluginManagerError> {
    instance().load_plugin_file_with_config(path, PluginRuntimeConfig::from_environment())
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
            PluginRuntimeConfig::from_environment()
        )
        .map(|report| report.loaded_count())
}
pub fn load_plugins_with_policy(
    plugin_path: &str,
    policy: PluginLoadPolicy
) -> Result<PluginLoadReport, PluginManagerError> {
    instance().load_plugins_with_policy_and_config(
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
    instance().load_database_plugins_with_config(PluginRuntimeConfig::from_environment())
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
