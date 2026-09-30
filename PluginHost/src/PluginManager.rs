use PluginContracts::{ ApiContract::{ PluginCatalog, PluginManifest }, IPlugin::IPlugin };
use super::Factory::PluginManagerError;
use glob::{ glob, GlobError };
use libloading::{ Library, Symbol };
use log::LevelFilter;
use serde::{ de::DeserializeOwned, Serialize };
use PluginAbi::{
    AbiBuffer,
    AbiSlice,
    PluginCreateFnV1,
    PluginEndpointFnV1,
    PluginHostServicesV1,
    PluginInstanceV1,
    PluginVTableV1,
    MAX_ABI_BUFFER_BYTES,
    PLUGIN_ABI_VERSION_V1,
    PLUGIN_CONSTRUCTOR_SYMBOL_V1,
    PLUGIN_STATUS_NOT_INITIALIZED,
    PLUGIN_STATUS_OK,
};
use std::{ collections::HashMap, path::{ Path, PathBuf }, sync::{ Arc, Mutex } };

const PLUGIN_AREA_ID: i64 = 0x00ff_ffff;

/// Runtime state of one loaded plugin.
///
/// The dynamic library and opaque plugin instance remain together so executable code cannot be
/// unloaded while a runtime handle is still reachable. All interaction with the plugin happens
/// through the versioned C-compatible ABI and serialized byte payloads.
pub(crate) struct PluginRuntime {
    instance: Option<*mut PluginInstanceV1>,
    #[allow(dead_code)]
    library: Library,
}

// Raw instance pointers are only touched while PluginRuntime is protected by its mutex. The
// plugin implementation remains Send + Sync internally, and the ABI exposes no Rust references.
unsafe impl Send for PluginRuntime {}
unsafe impl Sync for PluginRuntime {}

impl PluginRuntime {
    fn is_valid_buffer(buffer: AbiBuffer) -> bool {
        if buffer.ptr.is_null() {
            return buffer.len == 0 && buffer.capacity == 0;
        }

        buffer.len > 0 &&
            buffer.len <= MAX_ABI_BUFFER_BYTES &&
            buffer.capacity >= buffer.len &&
            buffer.capacity <= MAX_ABI_BUFFER_BYTES
    }

    fn new(instance: *mut PluginInstanceV1, library: Library) -> Result<Self, String> {
        if instance.is_null() {
            return Err("plugin constructor returned a null instance".to_owned());
        }

        let instance_ref = unsafe { &*instance };
        if
            instance_ref.abi_version != PLUGIN_ABI_VERSION_V1 ||
            instance_ref.struct_size < (std::mem::size_of::<PluginInstanceV1>() as u32) ||
            instance_ref.vtable.is_null() ||
            instance_ref.context.is_null()
        {
            return Err("plugin instance reported an incompatible ABI layout".to_owned());
        }

        let vtable = unsafe { &*instance_ref.vtable };
        if !vtable.is_compatible() {
            return Err("plugin vtable reported an incompatible ABI layout".to_owned());
        }

        let function_pointer_bytes = |pointer: usize| pointer != 0;
        let pointers = [
            vtable.name as usize,
            vtable.manifest as usize,
            vtable.on_load as usize,
            vtable.is_loaded as usize,
            vtable.handle_endpoint as usize,
            vtable.execute_database as usize,
            vtable.on_session_closed as usize,
            vtable.on_unload as usize,
            vtable.free_buffer as usize,
            vtable.destroy as usize,
        ];
        if pointers.iter().any(|pointer| !function_pointer_bytes(*pointer)) {
            return Err("plugin vtable contains a null function pointer".to_owned());
        }

        Ok(Self { instance: Some(instance), library })
    }

    pub(crate) fn execute<R, F>(runtime: &Arc<Mutex<Self>>, func: F) -> Option<R>
        where F: FnOnce(&mut Self) -> R
    {
        let mut runtime = runtime.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if runtime.instance.is_none() {
            return None;
        }
        Some(func(&mut runtime))
    }

    fn abi_parts_from_instance(
        instance: *mut PluginInstanceV1
    ) -> Result<(*mut std::ffi::c_void, &'static PluginVTableV1), String> {
        let instance_ref = unsafe { &*instance };
        if instance_ref.vtable.is_null() || instance_ref.context.is_null() {
            return Err("plugin runtime contains invalid ABI pointers".to_owned());
        }
        let vtable = unsafe { &*instance_ref.vtable };
        if !vtable.is_compatible() {
            return Err("plugin runtime contains an incompatible ABI vtable".to_owned());
        }
        Ok((instance_ref.context, vtable))
    }

    fn abi_parts(&self) -> Result<(*mut std::ffi::c_void, &PluginVTableV1), String> {
        let instance = self.instance.ok_or_else(|| "plugin runtime is unloaded".to_owned())?;
        Self::abi_parts_from_instance(instance)
    }

    fn take_buffer(&self, buffer: AbiBuffer) -> Result<Vec<u8>, String> {
        if !Self::is_valid_buffer(buffer) {
            return Err("plugin returned an invalid response buffer".to_owned());
        }

        let (_, vtable) = self.abi_parts()?;
        let bytes = (unsafe { buffer.to_vec() }).ok_or_else(||
            "plugin returned an invalid response buffer".to_owned()
        )?;
        unsafe {
            (vtable.free_buffer)(buffer);
        }
        Ok(bytes)
    }

    fn free_response_if_valid(&self, response: AbiBuffer) {
        if response.ptr.is_null() || !Self::is_valid_buffer(response) {
            return;
        }
        if let Ok((_, vtable)) = self.abi_parts() {
            unsafe {
                (vtable.free_buffer)(response);
            }
        }
    }

    fn call_buffer(
        &self,
        call: unsafe extern "C" fn(*mut std::ffi::c_void, *mut AbiBuffer) -> i32
    ) -> Result<Vec<u8>, String> {
        let (context, _) = self.abi_parts()?;
        let mut response = AbiBuffer::EMPTY;
        let status = unsafe { call(context, &mut response) };
        if status != PLUGIN_STATUS_OK {
            self.free_response_if_valid(response);
            return Err(format!("plugin ABI call failed with status {status}"));
        }
        self.take_buffer(response)
    }

    fn call_json<T: Serialize, R: DeserializeOwned>(
        &self,
        request: &T,
        call: PluginEndpointFnV1
    ) -> Result<R, String> {
        let request_bytes = serde_json
            ::to_vec(request)
            .map_err(|error| format!("failed to encode plugin request: {error}"))?;
        let (context, _) = self.abi_parts()?;
        let mut response = AbiBuffer::EMPTY;
        let status = unsafe { call(context, AbiSlice::from_bytes(&request_bytes), &mut response) };
        if status != PLUGIN_STATUS_OK {
            self.free_response_if_valid(response);
            return Err(format!("plugin ABI call failed with status {status}"));
        }
        let bytes = self.take_buffer(response)?;
        serde_json
            ::from_slice(&bytes)
            .map_err(|error| format!("failed to decode plugin response: {error}"))
    }

    pub(crate) fn name(&self) -> Result<String, String> {
        let (_, vtable) = self.abi_parts()?;
        let name = vtable.name;
        let bytes = self.call_buffer(name)?;
        serde_json
            ::from_slice(&bytes)
            .map_err(|error| format!("failed to decode plugin name: {error}"))
    }

    pub(crate) fn manifest(&self) -> Result<PluginManifest, String> {
        let (_, vtable) = self.abi_parts()?;
        let manifest = vtable.manifest;
        let bytes = self.call_buffer(manifest)?;
        serde_json
            ::from_slice(&bytes)
            .map_err(|error| format!("failed to decode plugin manifest: {error}"))
    }

    pub(crate) fn is_loaded(&self) -> Result<bool, String> {
        let (context, vtable) = self.abi_parts()?;
        let status = unsafe { (vtable.is_loaded)(context) };
        match status {
            PLUGIN_STATUS_OK => Ok(true),
            PLUGIN_STATUS_NOT_INITIALIZED => Ok(false),
            _ => Err(format!("plugin ABI is_loaded call failed with status {status}")),
        }
    }

    pub(crate) fn handle_endpoint(
        &self,
        request: PluginContracts::ApiContract::PluginEndpointRequest
    ) -> Result<
        PluginContracts::ApiContract::PluginEndpointResponse,
        PluginContracts::ApiContract::PluginEndpointError
    > {
        let (_, vtable) = self
            .abi_parts()
            .map_err(|error|
                PluginContracts::ApiContract::PluginEndpointError::internal(
                    "plugin_abi_error",
                    "The plugin ABI call failed",
                    &error
                )
            )?;
        self
            .call_json::<
                _,
                Result<
                    PluginContracts::ApiContract::PluginEndpointResponse,
                    PluginContracts::ApiContract::PluginEndpointError
                >
            >(&request, vtable.handle_endpoint)
            .map_err(|error|
                PluginContracts::ApiContract::PluginEndpointError::internal(
                    "plugin_abi_error",
                    "The plugin ABI response could not be decoded",
                    &error
                )
            )?
    }

    pub(crate) fn on_session_closed(
        &self,
        context: &PluginContracts::ApiContract::PluginSessionContext
    ) -> Result<(), PluginContracts::ApiContract::PluginLifecycleError> {
        let (_, vtable) = self
            .abi_parts()
            .map_err(|error|
                PluginContracts::ApiContract::PluginLifecycleError::new("plugin_abi_error", error)
            )?;
        self
            .call_json::<_, Result<(), PluginContracts::ApiContract::PluginLifecycleError>>(
                context,
                vtable.on_session_closed
            )
            .map_err(|error|
                PluginContracts::ApiContract::PluginLifecycleError::new("plugin_abi_error", error)
            )?
    }

    pub(crate) fn execute_database(
        &self,
        request: DatabaseContracts::DatabaseRequest
    ) -> Result<DatabaseContracts::DatabaseResponse, DatabaseContracts::DatabaseError> {
        let (_, vtable) = self
            .abi_parts()
            .map_err(|error|
                DatabaseContracts::DatabaseError::new(
                    DatabaseContracts::DatabaseErrorKind::Connection,
                    error
                )
            )?;

        self
            .call_json::<
                _,
                Result<DatabaseContracts::DatabaseResponse, DatabaseContracts::DatabaseError>
            >(&request, vtable.execute_database)
            .map_err(|error|
                DatabaseContracts::DatabaseError::new(
                    DatabaseContracts::DatabaseErrorKind::Decode,
                    error
                )
            )?
    }

    fn unload(&mut self) -> bool {
        let Some(instance) = self.instance else {
            return true;
        };

        let (context, on_unload, destroy) = match self.abi_parts() {
            Ok((context, vtable)) => (context, vtable.on_unload, vtable.destroy),
            Err(_) => {
                return false;
            }
        };

        let status = unsafe { on_unload(context) };
        if status != PLUGIN_STATUS_OK {
            return false;
        }

        self.instance = None;
        let destroy_status = unsafe { destroy(instance) };
        if destroy_status != PLUGIN_STATUS_OK {
            Common::Logging::Logger::Log::Dump(
                "{}",
                Common::Logging::LogConstants::LOGGERDEF,
                LevelFilter::Warn,
                &["Plugin instance destruction reported an ABI error."]
            );
        }
        true
    }
}

impl Drop for PluginRuntime {
    fn drop(&mut self) {
        let Some(instance) = self.instance else {
            return;
        };

        let (context, on_unload, destroy) = match self.abi_parts() {
            Ok((context, vtable)) => (context, vtable.on_unload, vtable.destroy),
            Err(_) => {
                return;
            }
        };

        let _ = unsafe { on_unload(context) };
        self.instance = None;
        let _ = unsafe { destroy(instance) };
    }
}

/// Public trait adapter preserving the existing `IPlugin`-based Factory API while the actual
/// dynamic-library boundary is represented only by `PluginRuntime` and the stable ABI.
#[derive(Clone)]
pub struct PluginProxy {
    runtime: Arc<Mutex<PluginRuntime>>,
}

impl PluginProxy {
    pub(crate) fn new(runtime: Arc<Mutex<PluginRuntime>>) -> Self {
        Self { runtime }
    }
}

impl IPlugin for PluginProxy {
    fn execute_database(
        &mut self,
        request: DatabaseContracts::DatabaseRequest
    ) -> Result<DatabaseContracts::DatabaseResponse, DatabaseContracts::DatabaseError> {
        PluginRuntime::execute(&self.runtime, |runtime|
            runtime.execute_database(request)
        ).unwrap_or_else(||
            Err(DatabaseContracts::DatabaseError::unsupported("plugin runtime is unavailable"))
        )
    }

    fn isLoaded(&self) -> bool {
        PluginRuntime::execute(&self.runtime, |runtime| runtime.is_loaded())
            .and_then(Result::ok)
            .unwrap_or(false)
    }

    fn manifest(&self) -> PluginManifest {
        PluginRuntime::execute(&self.runtime, |runtime| runtime.manifest())
            .and_then(Result::ok)
            .unwrap_or_else(|| PluginManifest::without_api("UnavailablePlugin"))
    }

    fn handle_endpoint(
        &mut self,
        request: PluginContracts::ApiContract::PluginEndpointRequest
    ) -> Result<
        PluginContracts::ApiContract::PluginEndpointResponse,
        PluginContracts::ApiContract::PluginEndpointError
    > {
        PluginRuntime::execute(&self.runtime, |runtime|
            runtime.handle_endpoint(request)
        ).unwrap_or_else(||
            Err(
                PluginContracts::ApiContract::PluginEndpointError::internal(
                    "plugin_unavailable",
                    "The plugin is no longer available",
                    "plugin runtime is unavailable"
                )
            )
        )
    }

    fn name(&self) -> String {
        PluginRuntime::execute(&self.runtime, |runtime| runtime.name())
            .and_then(Result::ok)
            .unwrap_or_else(|| "UnavailablePlugin".to_owned())
    }

    fn to_box(&self) -> Box<dyn IPlugin> {
        Box::new(self.clone())
    }

    fn on_load(&mut self) -> bool {
        self.isLoaded()
    }

    fn on_load_with_host(
        &mut self,
        _host_services: &PluginContracts::ApiContract::PluginHostServices
    ) -> bool {
        self.isLoaded()
    }

    fn on_unload(&mut self) -> bool {
        PluginRuntime::execute(&self.runtime, |runtime| runtime.unload()).unwrap_or(false)
    }

    fn on_session_closed(
        &mut self,
        context: &PluginContracts::ApiContract::PluginSessionContext
    ) -> Result<(), PluginContracts::ApiContract::PluginLifecycleError> {
        PluginRuntime::execute(&self.runtime, |runtime|
            runtime.on_session_closed(context)
        ).unwrap_or_else(||
            Err(
                PluginContracts::ApiContract::PluginLifecycleError::new(
                    "plugin_unavailable",
                    "plugin runtime is unavailable"
                )
            )
        )
    }
}

pub(crate) struct PluginItem {
    path: PathBuf,
    manifest: PluginManifest,
    runtime: Arc<Mutex<PluginRuntime>>,
}

impl PluginItem {
    pub(crate) fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    pub(crate) fn runtime(&self) -> Arc<Mutex<PluginRuntime>> {
        Arc::clone(&self.runtime)
    }

    pub(crate) fn unload(&mut self) -> bool {
        let mut runtime = self.runtime.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        runtime.unload()
    }
}

#[derive(Default)]
pub(crate) struct PluginManager {
    pub(crate) paths: HashMap<PathBuf, i64>,
    pub(crate) plugins: HashMap<i64, PluginItem>,
    catalog: PluginCatalog,
}

impl PluginManager {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn plugin_count(&self) -> usize {
        self.plugins.len()
    }

    pub(crate) fn get_runtime(&self, id: i64) -> Option<Arc<Mutex<PluginRuntime>>> {
        self.plugins.get(&id).map(PluginItem::runtime)
    }

    pub(crate) fn get_manifest(&self, id: i64) -> Option<&PluginManifest> {
        self.plugins.get(&id).map(PluginItem::manifest)
    }

    pub(crate) fn catalog(&self) -> PluginCatalog {
        self.catalog.clone()
    }

    pub(crate) fn plugin_ids(&self) -> Vec<i64> {
        let mut ids: Vec<i64> = self.plugins.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Discovers plugin library paths without loading or executing any plugin code.
    pub(crate) fn discover_plugin_paths(
        plugin_path: &str
    ) -> Result<Vec<Result<PathBuf, String>>, PluginManagerError> {
        let pattern = Self::build_plugin_pattern(plugin_path);
        let pattern_str = pattern
            .to_str()
            .ok_or_else(|| PluginManagerError::InvalidPluginPath(pattern.clone()))?;

        let entries = glob(pattern_str).map_err(|source| PluginManagerError::InvalidGlobPattern {
            pattern: pattern_str.to_owned(),
            source,
        })?;

        let mut paths = Vec::new();
        let mut errors = Vec::new();

        for entry in entries {
            match entry {
                Ok(path) => paths.push(Self::normalize_path(path.as_path())),
                Err(error) => errors.push(Err(format_glob_error(&error))),
            }
        }

        paths.sort();
        paths.dedup();

        let mut result = paths.into_iter().map(Ok).collect::<Vec<_>>();
        result.extend(errors);
        Ok(result)
    }

    /// Builds a candidate catalog without mutating manager state.
    pub(crate) fn catalog_with_manifest(
        &self,
        manifest: &PluginManifest
    ) -> Result<PluginCatalog, PluginManagerError> {
        let mut manifests = self
            .plugin_ids()
            .into_iter()
            .filter_map(|id| self.get_manifest(id).cloned())
            .collect::<Vec<_>>();
        manifests.push(manifest.clone());
        PluginCatalog::from_manifests(manifests).map_err(PluginManagerError::InvalidCatalog)
    }

    /// Registers a fully initialized plugin. No plugin callback is invoked here.
    pub(crate) fn insert_loaded_plugin(
        &mut self,
        canonical_path: &Path,
        loaded: LoadedPlugin,
        catalog: PluginCatalog
    ) -> i64 {
        let id = self.next_plugin_id();
        let item = PluginItem {
            path: canonical_path.to_path_buf(),
            manifest: loaded.manifest.clone(),
            runtime: Arc::new(Mutex::new(loaded.runtime)),
        };

        self.plugins.insert(id, item);
        self.paths.insert(canonical_path.to_path_buf(), id);
        self.catalog = catalog;
        id
    }

    /// Detaches a plugin from the registry without invoking plugin lifecycle code.
    pub(crate) fn detach_plugin(&mut self, id: i64) -> Result<PluginItem, PluginManagerError> {
        let manifests = self
            .plugin_ids()
            .into_iter()
            .filter(|plugin_id| *plugin_id != id)
            .filter_map(|plugin_id| self.get_manifest(plugin_id).cloned())
            .collect::<Vec<_>>();
        let new_catalog = PluginCatalog::from_manifests(manifests).map_err(
            PluginManagerError::InvalidCatalog
        )?;

        let item = self.plugins.remove(&id).ok_or(PluginManagerError::PluginNotFound { id })?;
        self.paths.remove(&item.path);
        self.catalog = new_catalog;
        Ok(item)
    }

    /// Restores a previously detached plugin. No plugin callback is invoked here.
    pub(crate) fn restore_plugin(&mut self, id: i64, item: PluginItem) {
        self.paths.insert(item.path.clone(), id);
        self.plugins.insert(id, item);
        let _ = self.rebuild_catalog();
    }

    fn rebuild_catalog(&mut self) -> Result<(), PluginManagerError> {
        let manifests = self
            .plugin_ids()
            .into_iter()
            .filter_map(|id| self.get_manifest(id).cloned())
            .collect::<Vec<_>>();
        self.catalog = PluginCatalog::from_manifests(manifests).map_err(
            PluginManagerError::InvalidCatalog
        )?;
        Ok(())
    }

    pub(crate) fn load_plugin(path: &Path) -> Result<LoadedPlugin, PluginManagerError> {
        let library = (unsafe { Library::new(path) }).map_err(|source| {
            PluginManagerError::LibraryLoad {
                path: path.to_path_buf(),
                source,
            }
        })?;

        let constructor: Symbol<PluginCreateFnV1> = (
            unsafe {
                library.get(PLUGIN_CONSTRUCTOR_SYMBOL_V1)
            }
        ).map_err(|source| PluginManagerError::ConstructorLoad {
            path: path.to_path_buf(),
            symbol: "InstantiateV1",
            source,
        })?;

        let plugin_directory = path.parent().unwrap_or_else(|| Path::new("."));
        let plugin_directory_bytes = plugin_directory.to_string_lossy().into_owned().into_bytes();
        let plugin_directory_slice = AbiSlice::from_bytes(&plugin_directory_bytes);
        let database = super::Factory::database_capability();
        let host_services = PluginHostServicesV1::new(
            host_log,
            database.map(|capability| capability.execute),
            database.map(|capability| capability.free),
            plugin_directory_slice
        );

        // The constructor performs plugin-local initialization before it returns. The borrowed
        // plugin directory therefore remains valid for every initialization callback without
        // requiring the plugin to retain host-owned memory.
        let instance = unsafe { constructor(&host_services) };
        let runtime = PluginRuntime::new(instance, library).map_err(|message| {
            PluginManagerError::PluginAbiIncompatible {
                path: path.to_path_buf(),
                message,
            }
        })?;

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

    fn next_plugin_id(&self) -> i64 {
        self.plugins
            .keys()
            .copied()
            .max()
            .map_or(PLUGIN_AREA_ID, |max_id| max_id + 1)
    }

    fn build_plugin_pattern(plugin_path: &str) -> PathBuf {
        Path::new(plugin_path).join(Self::library_glob())
    }

    fn library_glob() -> &'static str {
        #[cfg(target_os = "linux")]
        {
            "*.so"
        }
        #[cfg(target_os = "macos")]
        {
            "*.dylib"
        }
        #[cfg(target_os = "windows")]
        {
            "*.dll"
        }
    }

    pub(crate) fn normalize_path(path: &Path) -> PathBuf {
        path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
    }
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

    Common::Logging::Logger::Log::Dump(
        "{}",
        Common::Logging::LogConstants::LOGGERDEF,
        level_filter,
        &[message.as_str()]
    );
}

pub(crate) struct LoadedPlugin {
    runtime: PluginRuntime,
    manifest: PluginManifest,
}

impl LoadedPlugin {
    pub(crate) fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }
}

fn format_glob_error(error: &GlobError) -> String {
    match error.path().to_str() {
        Some(path) => format!("{} ({})", path, error.error()),
        None => error.error().to_string(),
    }
}
