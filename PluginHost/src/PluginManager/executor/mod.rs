//! Internal plugin execution boundary.
//!
//! The runtime owns scheduling, deadlines, admission, health, and lifecycle state. An executor
//! owns the mechanism used to run plugin code. The current implementation is in-process; a future
//! process-backed implementation can implement this contract without changing PluginRuntime.

mod in_process;
pub(super) use self::in_process::load_plugin;

use PluginAbi::PluginCancellationV2;
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
/// Plugin instance/function pointers, library handles, ABI buffer ownership, and ABI status
/// constants deliberately do not appear in this interface. `PluginCancellationV2` remains in the
/// contract for this behavior-preserving migration because the current callers already create it;
/// a future process executor must interpret cancellation locally and must never serialize its raw
/// `context` pointer across IPC.
pub(super) trait PluginExecutor: Send + Sync {
    /// Validates a newly constructed executor before runtime workers are created.
    fn validate(&self) -> Result<(), String>;

    /// Revalidates the executor before a lifecycle transition, retaining the existing runtime
    /// error messages for invalid in-process ABI state.
    fn validate_for_lifecycle(&self) -> Result<(), String>;

    fn name_bytes(&self, cancellation: PluginCancellationV2) -> Result<Vec<u8>, String>;
    fn manifest_bytes(&self, cancellation: PluginCancellationV2) -> Result<Vec<u8>, String>;
    fn is_loaded(&self, cancellation: PluginCancellationV2) -> Result<bool, String>;

    fn handle_endpoint(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2
    ) -> Result<Result<PluginEndpointResponse, PluginEndpointError>, String>;

    fn on_session_closed(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2
    ) -> Result<Result<(), PluginLifecycleError>, String>;

    /// Returns the decoded database result envelope. The outer error indicates an executor/ABI
    /// failure; the inner error is the datasource's domain-level `DatabaseError`.
    fn execute_database(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2
    ) -> Result<
        Result<DatabaseContracts::DatabaseResponse, DatabaseContracts::DatabaseError>,
        String
    >;

    /// Copies the serialized database envelope without decoding it, for the optimized database
    /// capability path.
    fn execute_database_bytes(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2
    ) -> Result<Vec<u8>, String>;

    fn on_unload(
        &self,
        cancellation: PluginCancellationV2
    ) -> Result<PluginLifecycleStatus, String>;
    fn destroy(&self, cancellation: PluginCancellationV2) -> Result<PluginLifecycleStatus, String>;

    /// Best-effort fallback used only when the runtime itself is dropped without a successful
    /// explicit unload. It preserves the previous in-process cleanup behavior.
    fn emergency_shutdown(&self);
}
