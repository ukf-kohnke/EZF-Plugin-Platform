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
};
pub use super::PluginManager::{ PluginHealthSnapshot, PluginHealthState, PluginRuntimeConfig };
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
use ApplicationConfig::plugin_root_dir;
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
pub(crate) use self::loading::find_id_by_name;
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
pub(crate) use self::database::database_capability;
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
use self::loading::{ plugin_operation_kind, PluginOperationKind };

struct SingletonReader {
    inner: Mutex<PluginManager>,
}

fn instance() -> &'static SingletonReader {
    static INSTANCE: OnceLock<SingletonReader> = OnceLock::new();
    INSTANCE.get_or_init(|| SingletonReader {
        inner: Mutex::new(PluginManager::new()),
    })
}
