//! Internal plugin execution boundary.
//!
//! The runtime owns scheduling, deadlines, admission, health, and lifecycle state. An executor
//! owns the mechanism used to run plugin code. In-process remains the default; an explicitly
//! selected process-backed adapter implements the same contract without changing PluginRuntime.

mod call_context;
mod in_process;
#[cfg(unix)]
mod process;

pub(super) use self::call_context::{
    PluginCallContext,
    PluginCancellationSource,
    PluginCancellationToken,
};
pub(crate) use self::call_context::PluginCancellationLocalState;
use super::{ LoadedPlugin, PluginRuntimeConfig };
use crate::Factory::PluginManagerError;
use PluginContracts::ApiContract::{
    PluginEndpointError,
    PluginEndpointResponse,
    PluginLifecycleError,
};

/// Runtime-level result of a lifecycle operation. ABI-specific success codes are translated by the
/// concrete executor so the runtime doesn't depend on a particular ABI's status constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PluginLifecycleStatus {
    Success,
    Failed(i32),
}

/// Operations understood by the runtime, independent of how plugin code is executed.
///
/// Plugin instance/function pointers, library handles, ABI cancellation pointers, ABI buffer
/// ownership, and ABI status constants deliberately do not appear in this interface. The context
/// is safe to inspect locally; process-backed executors must send only the operation id, remaining
/// budget, and cancellation messages across IPC.
pub(super) trait PluginExecutor: Send + Sync {
    /// Validates a newly constructed executor before runtime workers are created.
    fn validate(&self) -> Result<(), String>;

    /// Revalidates the executor before a lifecycle transition, retaining the existing runtime
    /// error messages for invalid in-process ABI state.
    fn validate_for_lifecycle(&self) -> Result<(), String>;

    fn name_bytes(&self, context: &PluginCallContext) -> Result<Vec<u8>, String>;
    fn manifest_bytes(&self, context: &PluginCallContext) -> Result<Vec<u8>, String>;
    fn is_loaded(&self, context: &PluginCallContext) -> Result<bool, String>;

    fn handle_endpoint(
        &self,
        request_bytes: &[u8],
        context: &PluginCallContext
    ) -> Result<Result<PluginEndpointResponse, PluginEndpointError>, String>;

    fn on_session_closed(
        &self,
        request_bytes: &[u8],
        context: &PluginCallContext
    ) -> Result<Result<(), PluginLifecycleError>, String>;

    /// Returns the decoded database result envelope. The outer error indicates an executor/ABI
    /// failure; the inner error is the datasource's domain-level `DatabaseError`.
    fn execute_database(
        &self,
        request_bytes: &[u8],
        context: &PluginCallContext
    ) -> Result<
        Result<DatabaseContracts::DatabaseResponse, DatabaseContracts::DatabaseError>,
        String
    >;

    /// Copies the serialized database envelope without decoding it, for the optimized database
    /// capability path.
    fn execute_database_bytes(
        &self,
        request_bytes: &[u8],
        context: &PluginCallContext
    ) -> Result<Vec<u8>, String>;

    fn on_unload(&self, context: &PluginCallContext) -> Result<PluginLifecycleStatus, String>;
    fn destroy(&self, context: &PluginCallContext) -> Result<PluginLifecycleStatus, String>;

    /// Best-effort fallback used only when the runtime itself is dropped without a successful
    /// explicit unload. It preserves the previous in-process cleanup behavior.
    fn emergency_shutdown(&self);
}

/// Selects the implementation behind the unchanged factory API. In-process execution remains the
/// default; process execution is an explicit local/integration opt-in until the capability broker
/// and enforced OS sandbox are implemented.
pub(super) fn load_plugin(
    path: &std::path::Path,
    config: PluginRuntimeConfig
) -> Result<LoadedPlugin, PluginManagerError> {
    match std::env::var("EZF_PLUGIN_EXECUTOR") {
        Ok(mode) if mode == "process" => {
            #[cfg(unix)]
            {
                process::load_plugin(path, config)
            }
            #[cfg(not(unix))]
            {
                Err(PluginManagerError::PluginAbiIncompatible {
                    path: path.to_path_buf(),
                    message: "process execution currently supports Unix-domain sockets only".to_owned(),
                })
            }
        }
        Ok(mode) if mode == "in_process" => in_process::load_plugin(path, config),
        Err(std::env::VarError::NotPresent) => in_process::load_plugin(path, config),
        Ok(other) =>
            Err(PluginManagerError::PluginAbiIncompatible {
                path: path.to_path_buf(),
                message: format!(
                    "unknown EZF_PLUGIN_EXECUTOR mode {other:?}; expected 'in_process' or 'process'"
                ),
            }),
        Err(error) =>
            Err(PluginManagerError::PluginAbiIncompatible {
                path: path.to_path_buf(),
                message: format!("could not read EZF_PLUGIN_EXECUTOR: {error}"),
            }),
    }
}
