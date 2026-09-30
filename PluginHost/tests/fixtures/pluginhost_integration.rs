#![allow(non_snake_case)]

use std::ffi::c_void;

const PLUGIN_ABI_VERSION_V1: u32 = 1;
const PLUGIN_STATUS_OK: i32 = 0;
const PLUGIN_STATUS_NOT_INITIALIZED: i32 = -3;

#[repr(C)]
#[derive(Clone, Copy)]
struct AbiSlice {
    ptr: *const u8,
    len: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct AbiBuffer {
    ptr: *mut u8,
    len: usize,
    capacity: usize,
}

impl AbiBuffer {
    fn json(value: &str) -> Self {
        let mut bytes = value.as_bytes().to_vec();
        let buffer = Self {
            ptr: bytes.as_mut_ptr(),
            len: bytes.len(),
            capacity: bytes.capacity(),
        };
        std::mem::forget(bytes);
        buffer
    }

    unsafe fn free(self) {
        if self.ptr.is_null() {
            return;
        }
        let _ = Vec::from_raw_parts(self.ptr, 0, self.capacity);
    }
}

type PluginLogFn = unsafe extern "C" fn(level: u8, message: AbiSlice);
type PluginDatabaseExecuteFn = unsafe extern "C" fn(
    request: AbiSlice,
    response_out: *mut AbiBuffer
) -> i32;
type PluginFreeBufferFn = unsafe extern "C" fn(buffer: AbiBuffer);
type PluginNameFn = unsafe extern "C" fn(context: *mut c_void, response_out: *mut AbiBuffer) -> i32;
type PluginManifestFn = unsafe extern "C" fn(
    context: *mut c_void,
    response_out: *mut AbiBuffer
) -> i32;
type PluginLoadFn = unsafe extern "C" fn(context: *mut c_void) -> i32;
type PluginLoadedFn = unsafe extern "C" fn(context: *mut c_void) -> i32;
type PluginEndpointFn = unsafe extern "C" fn(
    context: *mut c_void,
    request: AbiSlice,
    response_out: *mut AbiBuffer
) -> i32;
type PluginSessionClosedFn = unsafe extern "C" fn(
    context: *mut c_void,
    request: AbiSlice,
    response_out: *mut AbiBuffer
) -> i32;
type PluginDestroyFn = unsafe extern "C" fn(instance: *mut PluginInstanceV1) -> i32;

#[repr(C)]
#[derive(Clone, Copy)]
struct PluginLoggerV1 {
    emit: PluginLogFn,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct PluginHostServicesV1 {
    abi_version: u32,
    struct_size: u32,
    logger: PluginLoggerV1,
    database: Option<PluginDatabaseExecuteFn>,
    database_free: Option<PluginFreeBufferFn>,
    plugin_directory: AbiSlice,
}

#[repr(C)]
struct PluginVTableV1 {
    abi_version: u32,
    struct_size: u32,
    name: PluginNameFn,
    manifest: PluginManifestFn,
    on_load: PluginLoadFn,
    is_loaded: PluginLoadedFn,
    handle_endpoint: PluginEndpointFn,
    execute_database: PluginEndpointFn,
    on_session_closed: PluginSessionClosedFn,
    on_unload: PluginLoadFn,
    free_buffer: PluginFreeBufferFn,
    destroy: PluginDestroyFn,
}

#[repr(C)]
struct PluginInstanceV1 {
    abi_version: u32,
    struct_size: u32,
    context: *mut c_void,
    vtable: *const PluginVTableV1,
}

struct Context {
    loaded: bool,
}

unsafe extern "C" fn plugin_name(_context: *mut c_void, response_out: *mut AbiBuffer) -> i32 {
    if response_out.is_null() {
        return -1;
    }
    unsafe {
        *response_out = AbiBuffer::json(r#""PluginHostIntegration""#);
    }
    PLUGIN_STATUS_OK
}

unsafe extern "C" fn plugin_manifest(_context: *mut c_void, response_out: *mut AbiBuffer) -> i32 {
    if response_out.is_null() {
        return -1;
    }
    unsafe {
        *response_out = AbiBuffer::json(
            r#"{"contract_version":6,"name":"PluginHostIntegration","version":"0.0.1","kind":"Domain","functions":[],"endpoints":[{"id":"ping","method":"Get","path":"/pluginhost-integration/ping","consumes":null,"produces":"application/json","max_body_bytes":null,"request_body":null,"query_parameters":[],"tag":null,"summary":null,"description":null}]}"#
        );
    }
    PLUGIN_STATUS_OK
}

unsafe extern "C" fn plugin_on_load(context: *mut c_void) -> i32 {
    if context.is_null() {
        return -1;
    }
    unsafe {
        (*(context as *mut Context)).loaded = true;
    }
    PLUGIN_STATUS_OK
}

unsafe extern "C" fn plugin_is_loaded(context: *mut c_void) -> i32 {
    if context.is_null() {
        return -1;
    }
    if (unsafe { (*(context as *mut Context)).loaded }) {
        PLUGIN_STATUS_OK
    } else {
        PLUGIN_STATUS_NOT_INITIALIZED
    }
}

unsafe extern "C" fn plugin_handle_endpoint(
    context: *mut c_void,
    _request: AbiSlice,
    response_out: *mut AbiBuffer
) -> i32 {
    if context.is_null() || response_out.is_null() {
        return -1;
    }
    let loaded = unsafe { (*(context as *mut Context)).loaded };
    let response =
        format!(r#"{{"plugin":"PluginHostIntegration","loaded":{},"endpoint":"ping"}}"#, loaded);
    unsafe {
        *response_out = AbiBuffer::json(&response);
    }
    PLUGIN_STATUS_OK
}

unsafe extern "C" fn plugin_execute_database(
    _context: *mut c_void,
    _request: AbiSlice,
    response_out: *mut AbiBuffer
) -> i32 {
    if response_out.is_null() {
        return -1;
    }
    unsafe {
        *response_out = AbiBuffer::json("null");
    }
    PLUGIN_STATUS_OK
}

unsafe extern "C" fn plugin_on_session_closed(
    _context: *mut c_void,
    _request: AbiSlice,
    response_out: *mut AbiBuffer
) -> i32 {
    if response_out.is_null() {
        return -1;
    }
    unsafe {
        *response_out = AbiBuffer::json("null");
    }
    PLUGIN_STATUS_OK
}

unsafe extern "C" fn plugin_on_unload(context: *mut c_void) -> i32 {
    if context.is_null() {
        return -1;
    }
    unsafe {
        (*(context as *mut Context)).loaded = false;
    }
    PLUGIN_STATUS_OK
}

unsafe extern "C" fn plugin_free_buffer(buffer: AbiBuffer) {
    unsafe { buffer.free() }
}

unsafe extern "C" fn plugin_destroy(instance: *mut PluginInstanceV1) -> i32 {
    if instance.is_null() {
        return -1;
    }
    let instance = unsafe { Box::from_raw(instance) };
    if !instance.context.is_null() {
        unsafe {
            drop(Box::from_raw(instance.context as *mut Context));
        }
    }
    PLUGIN_STATUS_OK
}

static PLUGIN_VTABLE: PluginVTableV1 = PluginVTableV1 {
    abi_version: PLUGIN_ABI_VERSION_V1,
    struct_size: std::mem::size_of::<PluginVTableV1>() as u32,
    name: plugin_name,
    manifest: plugin_manifest,
    on_load: plugin_on_load,
    is_loaded: plugin_is_loaded,
    handle_endpoint: plugin_handle_endpoint,
    execute_database: plugin_execute_database,
    on_session_closed: plugin_on_session_closed,
    on_unload: plugin_on_unload,
    free_buffer: plugin_free_buffer,
    destroy: plugin_destroy,
};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn InstantiateV1(
    host_services: *const PluginHostServicesV1
) -> *mut PluginInstanceV1 {
    if host_services.is_null() {
        return std::ptr::null_mut();
    }

    let host = unsafe { &*host_services };
    if
        host.abi_version != PLUGIN_ABI_VERSION_V1 ||
        host.struct_size < (std::mem::size_of::<PluginHostServicesV1>() as u32)
    {
        return std::ptr::null_mut();
    }

    let context = Box::into_raw(Box::new(Context { loaded: false })) as *mut c_void;
    let instance = Box::new(PluginInstanceV1 {
        abi_version: PLUGIN_ABI_VERSION_V1,
        struct_size: std::mem::size_of::<PluginInstanceV1>() as u32,
        context,
        vtable: &PLUGIN_VTABLE,
    });
    let instance = Box::into_raw(instance);
    if plugin_on_load(context) != PLUGIN_STATUS_OK {
        unsafe {
            drop(Box::from_raw(context as *mut Context));
        }
        unsafe {
            drop(Box::from_raw(instance));
        }
        return std::ptr::null_mut();
    }
    instance
}
