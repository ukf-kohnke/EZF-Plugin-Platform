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
use super::runtime::PluginExecutorKind;
use crate::PluginHostInstance::{ DatabaseDispatcher, PluginManagerError };
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
    /// A process-backed executor confirmed worker termination/reaping before the full graceful
    /// lifecycle completed. The runtime may detach it, but must report cleanup as non-graceful.
    ForcedTermination,
}

pub(super) trait PluginExecutor: Send + Sync {
    /// Identifies the concrete executor without leaking executor internals into the runtime.
    fn kind(&self) -> PluginExecutorKind;

    /// Returns the current child worker PID for process-backed execution, if one is alive.
    fn worker_pid(&self) -> Option<u32> {
        None
    }

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

    /// Additional bounded wait after lifecycle cancellation so process-backed executors can finish
    /// their Cancel-Grace/Kill/Wait escalation. In-process callbacks cannot be safely killed and
    /// therefore retain the conservative zero-grace behavior.
    fn lifecycle_escalation_grace(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }

    /// True only when this executor can prove that its worker process has exited and been reaped.
    /// In-process executors must always return false.
    fn process_exit_confirmed(&self) -> bool {
        false
    }

    /// Best-effort fallback used only when the runtime itself is dropped without a successful
    /// explicit unload. It preserves the previous in-process cleanup behavior.
    fn emergency_shutdown(&self);
}

/// Selects the implementation behind the unchanged factory API. In-process execution remains the
/// default; process execution is an explicit local/integration opt-in until an enforced OS sandbox
/// is implemented. The IPC capability broker is implemented but does not itself provide isolation.
pub(super) fn load_plugin(
    path: &std::path::Path,
    config: PluginRuntimeConfig,
    selection: crate::PluginHostInstance::PluginExecutorSelection,
    process_config: crate::PluginHostInstance::ProcessExecutorConfig,
    database: Option<PluginContracts::ApiContract::PluginDatabaseCapability>,
    database_dispatcher: DatabaseDispatcher
) -> Result<LoadedPlugin, PluginManagerError> {
    match selection {
        crate::PluginHostInstance::PluginExecutorSelection::Process => {
            #[cfg(unix)]
            {
                process::load_plugin(path, config, process_config, database, database_dispatcher)
            }
            #[cfg(not(unix))]
            {
                let _ = (database, process_config, database_dispatcher);
                Err(PluginManagerError::PluginAbiIncompatible {
                    path: path.to_path_buf(),
                    message: "process execution currently supports Unix-domain sockets only".to_owned(),
                })
            }
        }
        crate::PluginHostInstance::PluginExecutorSelection::InProcess => {
            let _ = process_config;
            in_process::load_plugin(path, config, database, database_dispatcher)
        }
    }
}
