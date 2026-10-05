#![allow(non_snake_case)]

use std::{
    ffi::c_void,
    path::PathBuf,
    sync::atomic::{ AtomicUsize, Ordering },
    thread,
    time::Duration,
};

const PLUGIN_ABI_VERSION_V2: u32 = 2;
const PLUGIN_STATUS_OK: i32 = 0;
const PLUGIN_STATUS_NOT_INITIALIZED: i32 = -3;
const PLUGIN_NAME: &str = match option_env!("EZF_TEST_PLUGIN_NAME") {
    Some(name) => name,
    None => "PluginHostIntegration",
};

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

    fn free(self) {
        if self.ptr.is_null() {
            return;
        }
        let _ = unsafe { Vec::from_raw_parts(self.ptr, 0, self.capacity) };
    }
}

type PluginIsCancelledFnV2 = unsafe extern "C" fn(context: *const c_void) -> bool;

#[repr(C)]
#[derive(Clone, Copy)]
struct PluginCancellationV2 {
    context: *const c_void,
    is_cancelled: PluginIsCancelledFnV2,
}

type PluginLogFn = unsafe extern "C" fn(level: u8, message: AbiSlice);
type PluginDatabaseExecuteFn = unsafe extern "C" fn(
    request: AbiSlice,
    cancellation: PluginCancellationV2,
    response_out: *mut AbiBuffer
) -> i32;
type PluginDatabaseFn = unsafe extern "C" fn(
    context: *mut c_void,
    request: AbiSlice,
    cancellation: PluginCancellationV2,
    response_out: *mut AbiBuffer
) -> i32;
type PluginFreeBufferFn = unsafe extern "C" fn(buffer: AbiBuffer);
type PluginNameFn = unsafe extern "C" fn(
    context: *mut c_void,
    cancellation: PluginCancellationV2,
    response_out: *mut AbiBuffer
) -> i32;
type PluginManifestFn = unsafe extern "C" fn(
    context: *mut c_void,
    cancellation: PluginCancellationV2,
    response_out: *mut AbiBuffer
) -> i32;
type PluginLoadFn = unsafe extern "C" fn(
    context: *mut c_void,
    cancellation: PluginCancellationV2
) -> i32;
type PluginLoadedFn = unsafe extern "C" fn(
    context: *mut c_void,
    cancellation: PluginCancellationV2
) -> i32;
type PluginEndpointFn = unsafe extern "C" fn(
    context: *mut c_void,
    request: AbiSlice,
    cancellation: PluginCancellationV2,
    response_out: *mut AbiBuffer
) -> i32;
type PluginSessionClosedFn = unsafe extern "C" fn(
    context: *mut c_void,
    request: AbiSlice,
    cancellation: PluginCancellationV2,
    response_out: *mut AbiBuffer
) -> i32;
type PluginDestroyFn = unsafe extern "C" fn(
    instance: *mut PluginInstanceV2,
    cancellation: PluginCancellationV2
) -> i32;

#[repr(C)]
#[derive(Clone, Copy)]
struct PluginLoggerV2 {
    emit: PluginLogFn,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct PluginHostServicesV2 {
    abi_version: u32,
    struct_size: u32,
    logger: PluginLoggerV2,
    database: Option<PluginDatabaseExecuteFn>,
    database_free: Option<PluginFreeBufferFn>,
    plugin_directory: AbiSlice,
}

#[repr(C)]
struct PluginVTableV2 {
    abi_version: u32,
    struct_size: u32,
    name: PluginNameFn,
    manifest: PluginManifestFn,
    on_load: PluginLoadFn,
    is_loaded: PluginLoadedFn,
    handle_endpoint: PluginEndpointFn,
    execute_database: PluginDatabaseFn,
    on_session_closed: PluginSessionClosedFn,
    on_unload: PluginLoadFn,
    free_buffer: PluginFreeBufferFn,
    destroy: PluginDestroyFn,
}

#[repr(C)]
struct PluginInstanceV2 {
    abi_version: u32,
    struct_size: u32,
    context: *mut c_void,
    vtable: *const PluginVTableV2,
}

struct Context {
    loaded: bool,
    plugin_directory: PathBuf,
}

static ACTIVE_SLOW_CALLS: AtomicUsize = AtomicUsize::new(0);
static PEAK_SLOW_CALLS: AtomicUsize = AtomicUsize::new(0);

unsafe extern "C" fn plugin_name(
    _context: *mut c_void,
    _cancellation: PluginCancellationV2,
    response_out: *mut AbiBuffer
) -> i32 {
    if response_out.is_null() {
        return -1;
    }

    unsafe {
        *response_out = AbiBuffer::json(&format!(r#""{}""#, PLUGIN_NAME));
    }

    PLUGIN_STATUS_OK
}

unsafe extern "C" fn plugin_manifest(
    _context: *mut c_void,
    _cancellation: PluginCancellationV2,
    response_out: *mut AbiBuffer
) -> i32 {
    if response_out.is_null() {
        return -1;
    }
    unsafe {
        let manifest =
            format!(r#"{{"contract_version":7,"name":"{}","version":"0.0.1","kind":"Domain","endpoints":[{{"id":"ping","method":"Get","path":"/pluginhost-integration/ping","consumes":null,"produces":"application/json","max_body_bytes":null,"request_body":null,"query_parameters":[],"tag":null,"summary":null,"description":null}},{{"id":"slow","method":"Get","path":"/pluginhost-integration/slow","consumes":null,"produces":"application/json","max_body_bytes":null,"request_body":null,"query_parameters":[],"tag":null,"summary":null,"description":null}}]}}"#, PLUGIN_NAME);
        *response_out = AbiBuffer::json(&manifest);
    }

    PLUGIN_STATUS_OK
}

unsafe extern "C" fn plugin_on_load(
    context: *mut c_void,
    _cancellation: PluginCancellationV2
) -> i32 {
    if context.is_null() {
        return -1;
    }

    unsafe {
        (*(context as *mut Context)).loaded = true;
    }

    PLUGIN_STATUS_OK
}

unsafe extern "C" fn plugin_is_loaded(
    context: *mut c_void,
    _cancellation: PluginCancellationV2
) -> i32 {
    if context.is_null() {
        return -1;
    }

    unsafe {
        if (*(context as *mut Context)).loaded {
            PLUGIN_STATUS_OK
        } else {
            PLUGIN_STATUS_NOT_INITIALIZED
        }
    }
}

unsafe extern "C" fn plugin_handle_endpoint(
    context: *mut c_void,
    request: AbiSlice,
    cancellation: PluginCancellationV2,
    response_out: *mut AbiBuffer
) -> i32 {
    if context.is_null() || response_out.is_null() {
        return -1;
    }
    let context = unsafe { &*(context as *mut Context) };
    let request = (unsafe { request.to_vec() }).and_then(|bytes|
        serde_json::from_slice::<serde_json::Value>(&bytes).ok()
    );
    let endpoint_id = request
        .as_ref()
        .and_then(|value| value.get("endpoint_id"))
        .and_then(|value| value.as_str())
        .unwrap_or("ping");

    if endpoint_id == "slow" {
        let active = ACTIVE_SLOW_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
        PEAK_SLOW_CALLS.fetch_max(active, Ordering::SeqCst);
        let marker = context.plugin_directory.join("slow-entered.marker");
        let _ = std::fs::write(marker, b"entered");
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_millis(200) {
            if (unsafe { (cancellation.is_cancelled)(cancellation.context) }) {
                ACTIVE_SLOW_CALLS.fetch_sub(1, Ordering::SeqCst);
                return -6;
            }
            thread::sleep(Duration::from_millis(5));
        }
        ACTIVE_SLOW_CALLS.fetch_sub(1, Ordering::SeqCst);

        let peak = PEAK_SLOW_CALLS.load(Ordering::SeqCst);
        let response = format!(
            r#"{{"plugin":"{}","loaded":{},"endpoint":"slow","peak_active":{}}}"#,
            PLUGIN_NAME,
            context.loaded,
            peak
        );
        unsafe {
            *response_out = AbiBuffer::json(&response);
        }
        return PLUGIN_STATUS_OK;
    }

    let response = format!(
        r#"{{"plugin":"{}","loaded":{},"endpoint":"ping"}}"#,
        PLUGIN_NAME,
        context.loaded
    );

    unsafe {
        *response_out = AbiBuffer::json(&response);
    }
    PLUGIN_STATUS_OK
}

unsafe extern "C" fn plugin_execute_database(
    _context: *mut c_void,
    _request: AbiSlice,
    _cancellation: PluginCancellationV2,
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
    _cancellation: PluginCancellationV2,
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

unsafe extern "C" fn plugin_on_unload(
    context: *mut c_void,
    _cancellation: PluginCancellationV2
) -> i32 {
    if context.is_null() {
        return -1;
    }

    unsafe {
        (*(context as *mut Context)).loaded = false;
    }

    PLUGIN_STATUS_OK
}

unsafe extern "C" fn plugin_free_buffer(buffer: AbiBuffer) {
    buffer.free()
}

unsafe extern "C" fn plugin_destroy(
    instance: *mut PluginInstanceV2,
    _cancellation: PluginCancellationV2
) -> i32 {
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

static PLUGIN_VTABLE: PluginVTableV2 = PluginVTableV2 {
    abi_version: PLUGIN_ABI_VERSION_V2,
    struct_size: std::mem::size_of::<PluginVTableV2>() as u32,
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

unsafe extern "C" fn never_cancelled(_context: *const c_void) -> bool {
    false
}

#[unsafe(no_mangle)]
unsafe extern "C" fn InstantiateV2(
    host_services: *const PluginHostServicesV2
) -> *mut PluginInstanceV2 {
    if host_services.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        let host = &*host_services;

        if
            host.abi_version != PLUGIN_ABI_VERSION_V2 ||
            host.struct_size < (std::mem::size_of::<PluginHostServicesV2>() as u32)
        {
            return std::ptr::null_mut();
        }

        let plugin_directory = if host.plugin_directory.len == 0 {
            PathBuf::from(".")
        } else {
            let bytes = match host.plugin_directory.to_vec() {
                Some(bytes) => bytes,
                None => {
                    return std::ptr::null_mut();
                }
            };
            PathBuf::from(String::from_utf8_lossy(&bytes).into_owned())
        };
        let context = Box::into_raw(
            Box::new(Context {
                loaded: false,
                plugin_directory,
            })
        ) as *mut c_void;
        let instance = Box::new(PluginInstanceV2 {
            abi_version: PLUGIN_ABI_VERSION_V2,
            struct_size: std::mem::size_of::<PluginInstanceV2>() as u32,
            context,
            vtable: &PLUGIN_VTABLE,
        });
        let instance = Box::into_raw(instance);

        if
            plugin_on_load(context, PluginCancellationV2 {
                context: std::ptr::null(),
                is_cancelled: never_cancelled,
            }) != PLUGIN_STATUS_OK
        {
            drop(Box::from_raw(context as *mut Context));

            drop(Box::from_raw(instance));
            return std::ptr::null_mut();
        }
        instance
    }
}
