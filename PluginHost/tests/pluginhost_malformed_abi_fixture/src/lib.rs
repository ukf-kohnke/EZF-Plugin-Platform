#![allow(non_snake_case)]

use PluginAbi::{
    AbiBuffer,
    AbiSlice,
    PluginCancellationV2,
    PluginCreateFnV2,
    PluginHostServicesV2,
    PluginInstanceV2,
    PluginVTableV2,
    PLUGIN_ABI_VERSION_V2,
    PLUGIN_STATUS_NOT_INITIALIZED,
    PLUGIN_STATUS_OK,
};
use std::ffi::c_void;

unsafe extern "C" fn log(_level: u8, _message: AbiSlice) {}

unsafe fn json_string(value: &str, response_out: *mut AbiBuffer) -> i32 {
    if response_out.is_null() {
        return -1;
    }
    unsafe {
        *response_out = AbiBuffer::from_vec(value.as_bytes().to_vec());
    }
    PLUGIN_STATUS_OK
}

unsafe extern "C" fn plugin_name(_context: *mut c_void, _cancellation: PluginCancellationV2, response_out: *mut AbiBuffer) -> i32 {
    let json = serde_json::to_string("MalformedAbiFixture").expect("literal serializes");
    unsafe { json_string(&json, response_out) }
}

unsafe extern "C" fn plugin_manifest(_context: *mut c_void, _cancellation: PluginCancellationV2, response_out: *mut AbiBuffer) -> i32 {
    unsafe {
        json_string(
            r#"{"contract_version":6,"name":"MalformedAbiFixture","version":"0.0.1","kind":"Domain","functions":[],"endpoints":[]}"#,
            response_out
        )
    }
}

unsafe extern "C" fn on_load(_context: *mut c_void, _cancellation: PluginCancellationV2) -> i32 {
    PLUGIN_STATUS_OK
}
unsafe extern "C" fn is_loaded(_context: *mut c_void, _cancellation: PluginCancellationV2) -> i32 {
    PLUGIN_STATUS_NOT_INITIALIZED
}
unsafe extern "C" fn endpoint(
    _context: *mut c_void,
    _request: AbiSlice,
    _cancellation: PluginCancellationV2,
    response_out: *mut AbiBuffer
) -> i32 {
    unsafe { json_string("null", response_out) }
}
unsafe extern "C" fn database(
    _context: *mut c_void,
    _request: AbiSlice,
    _cancellation: PluginCancellationV2,
    response_out: *mut AbiBuffer
) -> i32 {
    unsafe { json_string("null", response_out) }
}
unsafe extern "C" fn session(
    _context: *mut c_void,
    _request: AbiSlice,
    _cancellation: PluginCancellationV2,
    response_out: *mut AbiBuffer
) -> i32 {
    unsafe { json_string("null", response_out) }
}
unsafe extern "C" fn unload(_context: *mut c_void, _cancellation: PluginCancellationV2) -> i32 {
    PLUGIN_STATUS_OK
}
unsafe extern "C" fn free_buffer(buffer: AbiBuffer) {
    unsafe {
        buffer.free();
    }
}
unsafe extern "C" fn destroy(instance: *mut PluginInstanceV2, _cancellation: PluginCancellationV2) -> i32 {
    if instance.is_null() {
        return -1;
    }
    unsafe {
        drop(Box::from_raw(instance));
    }
    PLUGIN_STATUS_OK
}

static VTABLE: PluginVTableV2 = PluginVTableV2 {
    abi_version: PLUGIN_ABI_VERSION_V2,
    struct_size: std::mem::size_of::<PluginVTableV2>() as u32,
    name: plugin_name,
    manifest: plugin_manifest,
    on_load,
    is_loaded,
    handle_endpoint: endpoint,
    execute_database: database,
    on_session_closed: session,
    on_unload: unload,
    free_buffer,
    destroy,
};

static BAD_VTABLE: PluginVTableV2 = PluginVTableV2 {
    abi_version: 999,
    struct_size: std::mem::size_of::<PluginVTableV2>() as u32,
    name: plugin_name,
    manifest: plugin_manifest,
    on_load,
    is_loaded,
    handle_endpoint: endpoint,
    execute_database: database,
    on_session_closed: session,
    on_unload: unload,
    free_buffer,
    destroy,
};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn InstantiateV2(
    host_services: *const PluginHostServicesV2
) -> *mut PluginInstanceV2 {
    if host_services.is_null() {
        return std::ptr::null_mut();
    }
    unsafe {
        log(3, AbiSlice::from_bytes(b"malformed ABI fixture loaded"));
    }

    let mode = std::env::var("EZF_MALFORMED_ABI_KIND").unwrap_or_default();
    if mode == "constructor_null" {
        return std::ptr::null_mut();
    }

    static DUMMY_CONTEXT: u8 = 0;
    let context = &DUMMY_CONTEXT as *const u8 as *mut c_void;
    let (abi_version, struct_size, context, vtable) = match mode.as_str() {
        "bad_instance_version" =>
            (
                999,
                std::mem::size_of::<PluginInstanceV2>() as u32,
                context,
                &VTABLE as *const PluginVTableV2,
            ),
        "bad_instance_size" =>
            (PLUGIN_ABI_VERSION_V2, 0, context, &VTABLE as *const PluginVTableV2),
        "null_context" =>
            (
                PLUGIN_ABI_VERSION_V2,
                std::mem::size_of::<PluginInstanceV2>() as u32,
                std::ptr::null_mut(),
                &VTABLE as *const PluginVTableV2,
            ),
        "null_vtable" =>
            (
                PLUGIN_ABI_VERSION_V2,
                std::mem::size_of::<PluginInstanceV2>() as u32,
                context,
                std::ptr::null::<PluginVTableV2>(),
            ),
        "bad_vtable_version" =>
            (
                PLUGIN_ABI_VERSION_V2,
                std::mem::size_of::<PluginInstanceV2>() as u32,
                context,
                &BAD_VTABLE as *const PluginVTableV2,
            ),
        "bad_vtable_size" => {
            static SMALL_VTABLE: PluginVTableV2 = PluginVTableV2 {
                abi_version: PLUGIN_ABI_VERSION_V2,
                struct_size: 0,
                name: plugin_name,
                manifest: plugin_manifest,
                on_load,
                is_loaded,
                handle_endpoint: endpoint,
                execute_database: database,
                on_session_closed: session,
                on_unload: unload,
                free_buffer,
                destroy,
            };
            (
                PLUGIN_ABI_VERSION_V2,
                std::mem::size_of::<PluginInstanceV2>() as u32,
                context,
                &SMALL_VTABLE as *const PluginVTableV2,
            )
        }
        _ =>
            (
                PLUGIN_ABI_VERSION_V2,
                std::mem::size_of::<PluginInstanceV2>() as u32,
                context,
                &VTABLE as *const PluginVTableV2,
            ),
    };

    Box::into_raw(
        Box::new(PluginInstanceV2 {
            abi_version,
            struct_size,
            context,
            vtable,
        })
    )
}

const _: PluginCreateFnV2 = InstantiateV2;
