//! Dynamic-library loading and ABI V2 execution for the current in-process backend.
//!
//! This module is intentionally the only PluginHost module that knows about `libloading`, raw
//! ABI instance/table pointers, and plugin-owned ABI buffers.
use super::{ PluginExecutor, PluginLifecycleStatus };
use super::super::{ LoadedPlugin, PluginRuntime, PluginRuntimeConfig };
use crate::Factory::PluginManagerError;
use libloading::{ Library, Symbol };
use log::LevelFilter;
use PluginAbi::{
    AbiBuffer,
    AbiSlice,
    PluginCancellationV2,
    PluginCreateFnV2,
    PluginHostServicesV2,
    PluginInstanceV2,
    PluginNameFnV2,
    PluginEndpointFnV2,
    PluginEndpointRawFnV2,
    PluginVTableV2,
    PluginVTableV2Ext,
    MAX_ABI_BUFFER_BYTES,
    PLUGIN_ABI_VERSION_V2,
    PLUGIN_CONSTRUCTOR_SYMBOL_V2,
    PLUGIN_STATUS_NOT_INITIALIZED,
    PLUGIN_STATUS_OK,
};
use PluginContracts::ApiContract::{
    PluginEndpointError,
    PluginEndpointResponse,
    PluginLifecycleError,
};
use std::{ ffi::c_void, path::Path, sync::Arc };

struct InProcessExecutor {
    instance: *mut PluginInstanceV2,
    // Keep the dynamic library loaded for as long as the opaque instance may be used.
    _library: Library,
}

// SAFETY: The ABI contract permits concurrent calls into a plugin instance; all access is guarded
// by PluginRuntime's read/write state lock and lifecycle gate. The raw pointer is never exposed.
unsafe impl Send for InProcessExecutor {}
unsafe impl Sync for InProcessExecutor {}

impl InProcessExecutor {
    fn new(instance: *mut PluginInstanceV2, library: Library) -> Self {
        Self { instance, _library: library }
    }

    fn is_valid_buffer(buffer: AbiBuffer) -> bool {
        if buffer.ptr.is_null() {
            return buffer.len == 0 && buffer.capacity == 0;
        }
        buffer.len > 0 &&
            buffer.len <= MAX_ABI_BUFFER_BYTES &&
            buffer.capacity >= buffer.len &&
            buffer.capacity <= MAX_ABI_BUFFER_BYTES
    }

    fn with_instance<R, F>(&self, func: F) -> Result<R, String>
        where F: FnOnce(*mut PluginInstanceV2, *mut c_void, &PluginVTableV2) -> Result<R, String>
    {
        if self.instance.is_null() {
            return Err("plugin runtime is unloaded".to_owned());
        }
        let instance_ref = unsafe { &*self.instance };
        if instance_ref.vtable.is_null() || instance_ref.context.is_null() {
            return Err("plugin runtime contains invalid ABI pointers".to_owned());
        }
        let vtable = unsafe { &*instance_ref.vtable };
        if !vtable.is_compatible() {
            return Err("plugin runtime contains an incompatible ABI vtable".to_owned());
        }
        func(self.instance, instance_ref.context, vtable)
    }

    fn call_buffer<F>(
        &self,
        cancellation: PluginCancellationV2,
        select_call: F
    ) -> Result<Vec<u8>, String>
        where F: FnOnce(&PluginVTableV2) -> PluginNameFnV2
    {
        self.with_instance(|_, context, vtable| {
            let mut response = AbiBuffer::EMPTY;
            let status = unsafe { select_call(vtable)(context, cancellation, &mut response) };
            if status != PLUGIN_STATUS_OK {
                if Self::is_valid_buffer(response) && !response.ptr.is_null() {
                    unsafe {
                        (vtable.free_buffer)(response);
                    }
                }
                return Err(format!("plugin ABI call failed with status {status}"));
            }
            if !Self::is_valid_buffer(response) {
                return Err("plugin returned an invalid response buffer".to_owned());
            }
            let bytes = unsafe { response.to_vec() };
            // Always release valid producer-owned memory through the plugin's allocator.
            unsafe {
                (vtable.free_buffer)(response);
            }
            bytes.ok_or_else(|| "plugin returned an invalid response buffer".to_owned())
        })
    }

    fn call_abi_bytes<F>(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2,
        select_call: F
    ) -> Result<Vec<u8>, String>
        where F: FnOnce(&PluginVTableV2) -> PluginEndpointFnV2
    {
        self.with_instance(|_, context, vtable| {
            let mut response = AbiBuffer::EMPTY;
            let status = unsafe {
                select_call(vtable)(
                    context,
                    AbiSlice::from_bytes(request_bytes),
                    cancellation,
                    &mut response
                )
            };
            if status != PLUGIN_STATUS_OK {
                Self::free_if_valid(vtable, response);
                return Err(format!("plugin ABI call failed with status {status}"));
            }
            if !Self::is_valid_buffer(response) {
                return Err("plugin returned an invalid response buffer".to_owned());
            }
            let bytes = unsafe { response.to_vec() };
            unsafe {
                (vtable.free_buffer)(response);
            }
            bytes.ok_or_else(|| "plugin returned an invalid response buffer".to_owned())
        })
    }

    fn call_json<R: serde::de::DeserializeOwned, F>(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2,
        select_call: F
    ) -> Result<R, String>
        where F: FnOnce(&PluginVTableV2) -> PluginEndpointFnV2
    {
        let bytes = self.call_abi_bytes(request_bytes, cancellation, select_call)?;
        serde_json
            ::from_slice(&bytes)
            .map_err(|error| format!("failed to decode plugin response: {error}"))
    }

    fn call_endpoint(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2
    ) -> Result<Result<PluginEndpointResponse, PluginEndpointError>, String> {
        self.with_instance(|_, context, vtable| {
            if let Some(raw_endpoint) = Self::raw_endpoint_callback(vtable) {
                let mut metadata = AbiBuffer::EMPTY;
                let mut body = AbiBuffer::EMPTY;
                let status = unsafe {
                    raw_endpoint(
                        context,
                        AbiSlice::from_bytes(request_bytes),
                        cancellation,
                        &mut metadata,
                        &mut body
                    )
                };
                if status != PLUGIN_STATUS_OK {
                    Self::free_if_valid(vtable, metadata);
                    Self::free_if_valid(vtable, body);
                    return Err(
                        format!("plugin ABI endpoint fast path failed with status {status}")
                    );
                }
                // Copy/free both buffers before propagating either error, so malformed metadata
                // cannot leak a valid body buffer (and vice versa).
                let metadata_bytes = Self::copy_and_free_buffer(vtable, metadata);
                let body_bytes = Self::copy_and_free_buffer(vtable, body);
                let metadata_bytes = metadata_bytes?;
                let body_bytes = body_bytes?;
                let mut response: Result<PluginEndpointResponse, PluginEndpointError> = serde_json
                    ::from_slice(&metadata_bytes)
                    .map_err(|error|
                        format!("failed to decode plugin endpoint metadata: {error}")
                    )?;
                match &mut response {
                    Ok(success) => {
                        if !success.body.is_empty() {
                            return Err(
                                "plugin endpoint fast path returned a body in metadata and body buffers".to_owned()
                            );
                        }
                        success.body = body_bytes;
                    }
                    Err(_) => {
                        if !body_bytes.is_empty() {
                            return Err(
                                "plugin endpoint fast path returned a body alongside an error".to_owned()
                            );
                        }
                    }
                }
                Ok(response)
            } else {
                let mut response = AbiBuffer::EMPTY;
                let status = unsafe {
                    (vtable.handle_endpoint)(
                        context,
                        AbiSlice::from_bytes(request_bytes),
                        cancellation,
                        &mut response
                    )
                };
                if status != PLUGIN_STATUS_OK {
                    Self::free_if_valid(vtable, response);
                    return Err(format!("plugin ABI call failed with status {status}"));
                }
                let bytes = Self::copy_and_free_buffer(vtable, response)?;
                serde_json
                    ::from_slice(&bytes)
                    .map_err(|error| format!("failed to decode plugin response: {error}"))
            }
        })
    }

    fn raw_endpoint_callback(vtable: &PluginVTableV2) -> Option<PluginEndpointRawFnV2> {
        if (vtable.struct_size as usize) < std::mem::size_of::<PluginVTableV2Ext>() {
            return None;
        }
        // SAFETY: the advertised table size proves that the append-only extension is readable.
        let extended = unsafe { &*(vtable as *const PluginVTableV2 as *const PluginVTableV2Ext) };
        extended.handle_endpoint_raw
    }

    fn free_if_valid(vtable: &PluginVTableV2, buffer: AbiBuffer) {
        if Self::is_valid_buffer(buffer) && !buffer.ptr.is_null() {
            unsafe { (vtable.free_buffer)(buffer) }
        }
    }

    fn copy_and_free_buffer(vtable: &PluginVTableV2, buffer: AbiBuffer) -> Result<Vec<u8>, String> {
        if !Self::is_valid_buffer(buffer) {
            return Err("plugin returned an invalid response buffer".to_owned());
        }
        let bytes = unsafe { buffer.to_vec() };
        unsafe {
            (vtable.free_buffer)(buffer);
        }
        bytes.ok_or_else(|| "plugin returned an invalid response buffer".to_owned())
    }

    fn never_cancelled() -> PluginCancellationV2 {
        PluginCancellationV2 {
            context: std::ptr::null(),
            is_cancelled: never_cancelled,
        }
    }
}

impl PluginExecutor for InProcessExecutor {
    fn validate(&self) -> Result<(), String> {
        if self.instance.is_null() {
            return Err("plugin constructor returned a null instance".to_owned());
        }
        let instance_ref = unsafe { &*self.instance };
        if
            instance_ref.abi_version != PLUGIN_ABI_VERSION_V2 ||
            instance_ref.struct_size < (std::mem::size_of::<PluginInstanceV2>() as u32) ||
            instance_ref.vtable.is_null() ||
            instance_ref.context.is_null()
        {
            return Err("plugin instance reported an incompatible ABI layout".to_owned());
        }
        let vtable = unsafe { &*instance_ref.vtable };
        if !vtable.is_compatible() {
            return Err("plugin vtable reported an incompatible ABI layout".to_owned());
        }
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
        if pointers.iter().any(|pointer| *pointer == 0) {
            return Err("plugin vtable contains a null function pointer".to_owned());
        }
        Ok(())
    }

    fn validate_for_lifecycle(&self) -> Result<(), String> {
        if self.instance.is_null() {
            return Err("plugin runtime is unloaded".to_owned());
        }
        let instance_ref = unsafe { &*self.instance };
        if instance_ref.vtable.is_null() || instance_ref.context.is_null() {
            return Err("plugin runtime contains invalid ABI pointers".to_owned());
        }
        let vtable = unsafe { &*instance_ref.vtable };
        if !vtable.is_compatible() {
            return Err("plugin runtime contains an incompatible ABI vtable".to_owned());
        }
        Ok(())
    }

    fn name_bytes(&self, cancellation: PluginCancellationV2) -> Result<Vec<u8>, String> {
        self.call_buffer(cancellation, |vtable| vtable.name)
    }

    fn manifest_bytes(&self, cancellation: PluginCancellationV2) -> Result<Vec<u8>, String> {
        self.call_buffer(cancellation, |vtable| vtable.manifest)
    }

    fn is_loaded(&self, cancellation: PluginCancellationV2) -> Result<bool, String> {
        self.with_instance(|_, context, vtable| {
            let status = unsafe { (vtable.is_loaded)(context, cancellation) };
            match status {
                PLUGIN_STATUS_OK => Ok(true),
                PLUGIN_STATUS_NOT_INITIALIZED => Ok(false),
                _ => Err(format!("plugin ABI is_loaded call failed with status {status}")),
            }
        })
    }

    fn handle_endpoint(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2
    ) -> Result<Result<PluginEndpointResponse, PluginEndpointError>, String> {
        self.call_endpoint(request_bytes, cancellation)
    }

    fn on_session_closed(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2
    ) -> Result<Result<(), PluginLifecycleError>, String> {
        self.call_json(request_bytes, cancellation, |vtable| vtable.on_session_closed)
    }

    fn execute_database(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2
    ) -> Result<
        Result<DatabaseContracts::DatabaseResponse, DatabaseContracts::DatabaseError>,
        String
    > {
        self.call_json(request_bytes, cancellation, |vtable| vtable.execute_database)
    }

    fn execute_database_bytes(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2
    ) -> Result<Vec<u8>, String> {
        self.call_abi_bytes(request_bytes, cancellation, |vtable| vtable.execute_database)
    }

    fn on_unload(
        &self,
        cancellation: PluginCancellationV2
    ) -> Result<PluginLifecycleStatus, String> {
        let status = self.with_instance(|_, context, vtable| {
            Ok(unsafe { (vtable.on_unload)(context, cancellation) })
        })?;
        Ok(
            if status == PLUGIN_STATUS_OK {
                PluginLifecycleStatus::Success
            } else {
                PluginLifecycleStatus::Failed(status)
            }
        )
    }

    fn destroy(&self, cancellation: PluginCancellationV2) -> Result<PluginLifecycleStatus, String> {
        self.validate_for_lifecycle()?;
        // Copy the callback pointer out in a short scope. `destroy` may deallocate the opaque
        // instance, so no Rust reference into that instance may remain live during the callback.
        let destroy = unsafe {
            let instance_ref = &*self.instance;
            let vtable = &*instance_ref.vtable;
            vtable.destroy
        };
        let status = unsafe { destroy(self.instance, cancellation) };
        Ok(
            if status == PLUGIN_STATUS_OK {
                PluginLifecycleStatus::Success
            } else {
                PluginLifecycleStatus::Failed(status)
            }
        )
    }

    fn emergency_shutdown(&self) {
        // Match the previous PluginRuntime::drop fallback: only null instance/table/context
        // pointers suppress callbacks; this cleanup path intentionally does not run full ABI
        // compatibility validation before best-effort shutdown.
        if self.instance.is_null() {
            return;
        }
        let callbacks = unsafe {
            let instance_ref = &*self.instance;
            if instance_ref.vtable.is_null() || instance_ref.context.is_null() {
                return;
            }
            let vtable = &*instance_ref.vtable;
            (instance_ref.context, vtable.on_unload, vtable.destroy)
        };
        let cancellation = Self::never_cancelled();
        let _ = unsafe { (callbacks.1)(callbacks.0, cancellation) };
        let _ = unsafe { (callbacks.2)(self.instance, cancellation) };
    }
}

pub fn load_plugin(
    path: &Path,
    config: PluginRuntimeConfig
) -> Result<LoadedPlugin, PluginManagerError> {
    let library = (unsafe { Library::new(path) }).map_err(|source| {
        PluginManagerError::LibraryLoad { path: path.to_path_buf(), source }
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

    // Keep the constructor symbol borrow scoped so the library can move into the executor after
    // construction; the library remains loaded for the full lifetime of the executor.
    let instance = {
        let constructor: Symbol<PluginCreateFnV2> = (
            unsafe {
                library.get(PLUGIN_CONSTRUCTOR_SYMBOL_V2)
            }
        ).map_err(|source| PluginManagerError::ConstructorLoad {
            path: path.to_path_buf(),
            symbol: "InstantiateV2",
            source,
        })?;
        unsafe { constructor(&host_services) }
    };

    let executor: Arc<dyn PluginExecutor> = Arc::new(InProcessExecutor::new(instance, library));
    let runtime = PluginRuntime::new(executor, config).map_err(|message| {
        PluginManagerError::PluginAbiIncompatible { path: path.to_path_buf(), message }
    })?;

    // Metadata is untrusted plugin code too; the runtime supplies the same bounded call path,
    // timeout, panic isolation, and lifecycle/accounting controls as endpoint operations.
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
        _ => LevelFilter::Error,
    };
    crate::emit_log(level_filter, message.as_str());
}

unsafe extern "C" fn never_cancelled(_context: *const c_void) -> bool {
    false
}
