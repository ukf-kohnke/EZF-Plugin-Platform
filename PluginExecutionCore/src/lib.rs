//! Shared dynamic-library and Plugin ABI V2 execution primitives.
//!
//! This crate deliberately has no dependency on PluginHost, PluginRuntime, backend configuration,
//! the plugin registry, or host capability implementations. The caller supplies the process-local
//! `PluginHostServicesV2` callbacks; this crate owns the loaded library for as long as the instance
//! may be called and centralizes ABI layout validation and producer-owned buffer handling.
#![allow(non_snake_case)]
use libloading::{ Library, Symbol };
use PluginAbi::{
    AbiBuffer,
    AbiSlice,
    PluginCancellationV2,
    PluginCreateFnV2,
    PluginEndpointFnV2,
    PluginEndpointRawFnV2,
    PluginHostServicesV2,
    PluginInstanceV2,
    PluginNameFnV2,
    PluginVTableV2,
    PluginVTableV2Ext,
    MAX_ABI_BUFFER_BYTES,
    PLUGIN_ABI_VERSION_V2,
    PLUGIN_CONSTRUCTOR_SYMBOL_V2,
    PLUGIN_STATUS_NOT_INITIALIZED,
    PLUGIN_STATUS_OK,
};
use std::{ ffi::c_void, path::Path };

/// Identifies which step failed while loading a plugin so the host can preserve its public errors.
#[derive(Debug)]
pub enum PluginLoadError {
    Library(libloading::Error),
    Constructor(libloading::Error),
}

/// ABI-owned endpoint output after the producer buffers have been validated, copied and freed.
/// The in-process host adapter decodes the contract envelope; a future worker adapter can apply
/// the same interpretation before serializing the result over IPC.
pub enum EndpointCallOutput {
    Serialized(Vec<u8>),
    Raw {
        metadata: Vec<u8>,
        body: Vec<u8>,
    },
}

/// One loaded Plugin ABI V2 instance, including the library that owns its code and static data.
///
/// All raw pointers remain private. `Send` and `Sync` rely on the Plugin ABI V2 contract allowing
/// concurrent calls to the instance; the host runtime must continue to quiesce calls before
/// lifecycle callbacks and before dropping this value.
pub struct PluginExecutionCore {
    instance: *mut PluginInstanceV2,
    _library: Library,
}

// SAFETY: The ABI contract permits concurrent calls into an instance. The pointer and vtable are
// never exposed, lifecycle operations remain the caller's responsibility, and the library is kept
// alive in the same value as the instance.
unsafe impl Send for PluginExecutionCore {}
unsafe impl Sync for PluginExecutionCore {}

impl PluginExecutionCore {
    /// Loads a library and invokes its V2 constructor with caller-provided, process-local services.
    ///
    /// The service callbacks and all pointers referenced by them must remain valid according to
    /// the ABI contract. This method does not register or initialize the plugin in a host runtime.
    pub fn load(
        path: &Path,
        host_services: &PluginHostServicesV2
    ) -> Result<Self, PluginLoadError> {
        let library = (unsafe { Library::new(path) }).map_err(PluginLoadError::Library)?;

        // Keep the constructor symbol borrow scoped so the library can be moved into the returned
        // value after construction; the library remains loaded for the instance's full lifetime.
        let instance = {
            let constructor: Symbol<PluginCreateFnV2> = (
                unsafe {
                    library.get(PLUGIN_CONSTRUCTOR_SYMBOL_V2)
                }
            ).map_err(PluginLoadError::Constructor)?;
            unsafe { constructor(host_services as *const PluginHostServicesV2) }
        };

        Ok(Self { instance, _library: library })
    }

    /// Performs the full validation used after constructing an executor.
    pub fn validate(&self) -> Result<(), String> {
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

    /// Performs the intentionally narrower validation used by lifecycle retry paths.
    pub fn validate_for_lifecycle(&self) -> Result<(), String> {
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

    pub fn name_bytes(&self, cancellation: PluginCancellationV2) -> Result<Vec<u8>, String> {
        self.call_buffer(cancellation, |vtable| vtable.name)
    }

    pub fn manifest_bytes(&self, cancellation: PluginCancellationV2) -> Result<Vec<u8>, String> {
        self.call_buffer(cancellation, |vtable| vtable.manifest)
    }

    pub fn is_loaded(&self, cancellation: PluginCancellationV2) -> Result<bool, String> {
        self.with_instance(|_, context, vtable| {
            let status = unsafe { (vtable.is_loaded)(context, cancellation) };
            match status {
                PLUGIN_STATUS_OK => Ok(true),
                PLUGIN_STATUS_NOT_INITIALIZED => Ok(false),
                _ => Err(format!("plugin ABI is_loaded call failed with status {status}")),
            }
        })
    }

    /// Invokes the endpoint and returns host-owned bytes. For the optional raw endpoint ABI
    /// extension, both buffers are validated, copied and released here, preserving one shared
    /// implementation of the producer-allocator rules without depending on host contracts.
    pub fn handle_endpoint(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2
    ) -> Result<EndpointCallOutput, String> {
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
                Ok(EndpointCallOutput::Raw {
                    metadata: metadata_bytes,
                    body: body_bytes,
                })
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
                Ok(EndpointCallOutput::Serialized(bytes))
            }
        })
    }

    pub fn on_session_closed_bytes(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2
    ) -> Result<Vec<u8>, String> {
        self.call_abi_bytes(request_bytes, cancellation, |vtable| vtable.on_session_closed)
    }

    pub fn execute_database_bytes(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2
    ) -> Result<Vec<u8>, String> {
        self.call_abi_bytes(request_bytes, cancellation, |vtable| vtable.execute_database)
    }

    pub fn on_unload(&self, cancellation: PluginCancellationV2) -> Result<i32, String> {
        self.with_instance(|_, context, vtable| {
            Ok(unsafe { (vtable.on_unload)(context, cancellation) })
        })
    }

    pub fn destroy(&self, cancellation: PluginCancellationV2) -> Result<i32, String> {
        self.validate_for_lifecycle()?;
        // Copy the callback pointer out in a short scope. `destroy` may deallocate the opaque
        // instance, so no Rust reference into that instance may remain live during the callback.
        let destroy = unsafe {
            let instance_ref = &*self.instance;
            let vtable = &*instance_ref.vtable;
            vtable.destroy
        };
        Ok(unsafe { destroy(self.instance, cancellation) })
    }

    /// Best-effort fallback matching the old runtime-drop behavior. It intentionally skips full
    /// ABI compatibility checks and is only used when explicit unload did not complete.
    pub fn emergency_shutdown(&self) {
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
            Self::copy_and_free_buffer(vtable, response)
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

    fn is_valid_buffer(buffer: AbiBuffer) -> bool {
        if buffer.ptr.is_null() {
            return buffer.len == 0 && buffer.capacity == 0;
        }
        buffer.len > 0 &&
            buffer.len <= MAX_ABI_BUFFER_BYTES &&
            buffer.capacity >= buffer.len &&
            buffer.capacity <= MAX_ABI_BUFFER_BYTES
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

unsafe extern "C" fn never_cancelled(_context: *const c_void) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::PluginExecutionCore;
    use PluginAbi::{ AbiBuffer, MAX_ABI_BUFFER_BYTES };

    #[test]
    fn abi_buffer_validation_keeps_empty_and_bounded_buffers_valid() {
        assert!(PluginExecutionCore::is_valid_buffer(AbiBuffer::EMPTY));

        let valid = AbiBuffer::from_vec(vec![1, 2, 3]);
        assert!(PluginExecutionCore::is_valid_buffer(valid));
        unsafe {
            valid.free();
        }

        let null_non_empty = AbiBuffer { ptr: std::ptr::null_mut(), len: 1, capacity: 1 };
        assert!(!PluginExecutionCore::is_valid_buffer(null_non_empty));

        let oversized = AbiBuffer {
            ptr: 1usize as *mut u8,
            len: MAX_ABI_BUFFER_BYTES + 1,
            capacity: MAX_ABI_BUFFER_BYTES + 1,
        };
        assert!(!PluginExecutionCore::is_valid_buffer(oversized));
    }
}
