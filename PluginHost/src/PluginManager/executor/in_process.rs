//! In-process adapter for the shared Plugin ABI V2 execution core.
//!
//! This module supplies BackendSrv-specific host services and maps the runtime's ABI-neutral
//! cancellation context to a process-local ABI descriptor. Library loading, VTable validation,
//! ABI calls, and producer-owned buffer handling live in `PluginExecutionCore`.
use super::{ PluginCallContext, PluginCancellationToken, PluginExecutor, PluginLifecycleStatus };
use super::super::{ LoadedPlugin, PluginRuntime, PluginRuntimeConfig };
use crate::Factory::PluginManagerError;
use log::LevelFilter;
use PluginAbi::{ AbiSlice, PluginCancellationV2, PluginHostServicesV2, PLUGIN_STATUS_OK };
use PluginContracts::ApiContract::{
    PluginEndpointError,
    PluginEndpointResponse,
    PluginLifecycleError,
};
use PluginExecutionCore::{ EndpointCallOutput, PluginLoadError };
use std::{ ffi::c_void, path::Path, sync::Arc };

struct InProcessExecutor {
    core: PluginExecutionCore::PluginExecutionCore,
}

impl InProcessExecutor {
    fn new(core: PluginExecutionCore::PluginExecutionCore) -> Self {
        Self { core }
    }

    /// Maps the host-neutral cancellation token to the ABI descriptor only for the synchronous
    /// duration of one in-process callback. The pointer is never exposed by the executor contract
    /// or retained after this adapter returns.
    fn with_abi_cancellation<R, F>(&self, call_context: &PluginCallContext, call: F) -> R
        where F: FnOnce(PluginCancellationV2) -> R
    {
        let token = call_context.cancellation_token();
        let cancellation = PluginCancellationV2 {
            context: (&token as *const PluginCancellationToken).cast(),
            is_cancelled: plugin_cancellation_is_cancelled,
        };
        call(cancellation)
    }

    fn decode_response<R: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<R, String> {
        serde_json
            ::from_slice(bytes)
            .map_err(|error| format!("failed to decode plugin response: {error}"))
    }

    fn decode_endpoint_output(
        output: EndpointCallOutput
    ) -> Result<Result<PluginEndpointResponse, PluginEndpointError>, String> {
        match output {
            EndpointCallOutput::Serialized(bytes) => Self::decode_response(&bytes),
            EndpointCallOutput::Raw { metadata, body } => {
                let mut response: Result<PluginEndpointResponse, PluginEndpointError> = serde_json
                    ::from_slice(&metadata)
                    .map_err(|error| {
                        format!("failed to decode plugin endpoint metadata: {error}")
                    })?;
                match &mut response {
                    Ok(success) => {
                        if !success.body.is_empty() {
                            return Err(
                                "plugin endpoint fast path returned a body in metadata and body buffers".to_owned()
                            );
                        }
                        success.body = body;
                    }
                    Err(_) => {
                        if !body.is_empty() {
                            return Err(
                                "plugin endpoint fast path returned a body alongside an error".to_owned()
                            );
                        }
                    }
                }
                Ok(response)
            }
        }
    }
}

impl PluginExecutor for InProcessExecutor {
    fn validate(&self) -> Result<(), String> {
        self.core.validate()
    }

    fn validate_for_lifecycle(&self) -> Result<(), String> {
        self.core.validate_for_lifecycle()
    }

    fn name_bytes(&self, context: &PluginCallContext) -> Result<Vec<u8>, String> {
        self.with_abi_cancellation(context, |cancellation| self.core.name_bytes(cancellation))
    }

    fn manifest_bytes(&self, context: &PluginCallContext) -> Result<Vec<u8>, String> {
        self.with_abi_cancellation(context, |cancellation| self.core.manifest_bytes(cancellation))
    }

    fn is_loaded(&self, context: &PluginCallContext) -> Result<bool, String> {
        self.with_abi_cancellation(context, |cancellation| self.core.is_loaded(cancellation))
    }

    fn handle_endpoint(
        &self,
        request_bytes: &[u8],
        context: &PluginCallContext
    ) -> Result<Result<PluginEndpointResponse, PluginEndpointError>, String> {
        let output = self.with_abi_cancellation(context, |cancellation| {
            self.core.handle_endpoint(request_bytes, cancellation)
        })?;
        Self::decode_endpoint_output(output)
    }

    fn on_session_closed(
        &self,
        request_bytes: &[u8],
        context: &PluginCallContext
    ) -> Result<Result<(), PluginLifecycleError>, String> {
        let bytes = self.with_abi_cancellation(context, |cancellation| {
            self.core.on_session_closed_bytes(request_bytes, cancellation)
        })?;
        Self::decode_response(&bytes)
    }

    fn execute_database(
        &self,
        request_bytes: &[u8],
        context: &PluginCallContext
    ) -> Result<
        Result<DatabaseContracts::DatabaseResponse, DatabaseContracts::DatabaseError>,
        String
    > {
        let bytes = self.with_abi_cancellation(context, |cancellation| {
            self.core.execute_database_bytes(request_bytes, cancellation)
        })?;
        Self::decode_response(&bytes)
    }

    fn execute_database_bytes(
        &self,
        request_bytes: &[u8],
        context: &PluginCallContext
    ) -> Result<Vec<u8>, String> {
        self.with_abi_cancellation(context, |cancellation| {
            self.core.execute_database_bytes(request_bytes, cancellation)
        })
    }

    fn on_unload(&self, context: &PluginCallContext) -> Result<PluginLifecycleStatus, String> {
        let status = self.with_abi_cancellation(context, |cancellation| {
            self.core.on_unload(cancellation)
        })?;
        Ok(
            if status == PLUGIN_STATUS_OK {
                PluginLifecycleStatus::Success
            } else {
                PluginLifecycleStatus::Failed(status)
            }
        )
    }

    fn destroy(&self, context: &PluginCallContext) -> Result<PluginLifecycleStatus, String> {
        let status = self.with_abi_cancellation(context, |cancellation| {
            self.core.destroy(cancellation)
        })?;
        Ok(
            if status == PLUGIN_STATUS_OK {
                PluginLifecycleStatus::Success
            } else {
                PluginLifecycleStatus::Failed(status)
            }
        )
    }

    fn emergency_shutdown(&self) {
        self.core.emergency_shutdown();
    }
}

pub fn load_plugin(
    path: &Path,
    config: PluginRuntimeConfig
) -> Result<LoadedPlugin, PluginManagerError> {
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

    let core = PluginExecutionCore::PluginExecutionCore::load(path, &host_services).map_err(
        |source| {
            match source {
                PluginLoadError::Library(source) =>
                    PluginManagerError::LibraryLoad {
                        path: path.to_path_buf(),
                        source,
                    },
                PluginLoadError::Constructor(source) =>
                    PluginManagerError::ConstructorLoad {
                        path: path.to_path_buf(),
                        symbol: "InstantiateV2",
                        source,
                    },
            }
        }
    )?;

    let executor: Arc<dyn PluginExecutor> = Arc::new(InProcessExecutor::new(core));
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

unsafe extern "C" fn plugin_cancellation_is_cancelled(context: *const c_void) -> bool {
    if context.is_null() {
        return true;
    }
    let token = unsafe { &*(context as *const PluginCancellationToken) };
    token.is_cancelled()
}
