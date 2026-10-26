//! Dynamic-library loading and ABI V2 execution for the current in-process runtime.
use super::super::{ LoadedPlugin, PluginRuntime, PluginRuntimeConfig };
use crate::Factory::PluginManagerError;
use libloading::{ Library, Symbol };
use PluginAbi::{ AbiSlice, PluginCreateFnV2, PluginHostServicesV2, PLUGIN_CONSTRUCTOR_SYMBOL_V2 };
use std::path::Path;
use log::LevelFilter;

pub(crate) fn load_plugin(
    path: &Path,
    config: PluginRuntimeConfig
) -> Result<LoadedPlugin, PluginManagerError> {
    let library = (unsafe { Library::new(path) }).map_err(|source| {
        PluginManagerError::LibraryLoad {
            path: path.to_path_buf(),
            source,
        }
    })?;

    let constructor: Symbol<PluginCreateFnV2> = (
        unsafe {
            library.get(PLUGIN_CONSTRUCTOR_SYMBOL_V2)
        }
    ).map_err(|source| PluginManagerError::ConstructorLoad {
        path: path.to_path_buf(),
        symbol: "InstantiateV2",
        source,
    })?;

    let plugin_directory = path.parent().unwrap_or_else(|| Path::new("."));
    let plugin_directory_bytes = plugin_directory.to_string_lossy().into_owned().into_bytes();
    let plugin_directory_slice = AbiSlice::from_bytes(&plugin_directory_bytes);
    let database = crate::Factory::database_capability();
    let host_services = PluginHostServicesV2::new(
        host_log,
        database.map(|capability| capability.execute),
        database.map(|capability| capability.free),
        plugin_directory_slice
    );

    // The constructor performs plugin-local initialization before it returns. The borrowed
    // plugin directory therefore remains valid for every initialization callback without
    // requiring the plugin to retain host-owned memory.
    let instance = unsafe { constructor(&host_services) };
    let runtime = PluginRuntime::new(instance, library, config).map_err(|message| {
        PluginManagerError::PluginAbiIncompatible {
            path: path.to_path_buf(),
            message,
        }
    })?;

    // Metadata is untrusted plugin code too. Both calls execute through the same bounded
    // worker pool, timeout, panic-isolation, and lifecycle/accounting path as endpoint calls.
    let plugin_name = runtime.name().map_err(|message| PluginManagerError::PluginAbiCall {
        path: path.to_path_buf(),
        message,
    })?;
    let manifest = runtime.manifest().map_err(|message| PluginManagerError::PluginAbiCall {
        path: path.to_path_buf(),
        message,
    })?;

    if !plugin_name.eq_ignore_ascii_case(&manifest.name) {
        return Err(PluginManagerError::PluginNameMismatch {
            path: path.to_path_buf(),
            runtime_name: plugin_name,
            manifest_name: manifest.name.clone(),
        });
    }

    Ok(LoadedPlugin { runtime, manifest })
}

unsafe extern "C" fn host_log(level: u8, message: PluginAbi::AbiSlice) {
    let message = (unsafe { message.to_vec() })
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_else(|| "<plugin logger supplied an invalid message buffer>".to_owned());

    let level_filter = match level {
        1 => LevelFilter::Error,
        2 => LevelFilter::Warn,
        3 => LevelFilter::Info,
        4 => LevelFilter::Debug,
        5 => LevelFilter::Trace,
        _ => { LevelFilter::Error }
    };

    crate::emit_log(level_filter, message.as_str());
}
