//! Public plugin health snapshots and internal health state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PluginHealthState {
    Loading = 0,
    Ready = 1,
    Degraded = 2,
    Stopping = 3,
    Failed = 4,
    Unloaded = 5,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginHealthSnapshot {
    pub name: String,
    pub state: PluginHealthState,
    /// Configured number of persistent workers for this plugin.
    pub worker_threads: usize,
    /// Maximum number of admitted calls, including calls waiting for a worker.
    pub max_concurrent_calls: usize,
    /// Configured capacity of the worker queue.
    pub queue_capacity: usize,
    /// Jobs currently waiting in the bounded worker queue.
    pub queued_jobs: usize,
    /// Jobs currently executing on plugin worker threads.
    pub running_workers: usize,
    /// Calls admitted by the runtime that have not yet completed inside the plugin.
    pub active_calls: usize,
    /// Admitted calls for which the caller already received a timeout, while the plugin operation
    /// itself is still running or queued.
    pub timed_out_active_calls: u64,
    /// Calls that were actually completed by a plugin worker (success or failure).
    pub completed_calls: u64,
    /// Calls whose plugin operation actually failed or panicked. Request timeouts are not counted
    /// here because a timed-out operation may still complete successfully later.
    pub failed_calls: u64,
    /// Caller-visible request timeouts. This is a cumulative counter and is independent from the
    /// eventual outcome of the still-running plugin operation.
    pub timed_out_calls: u64,
    pub total_calls: u64,
    pub rejected_calls: u64,
    pub last_error: Option<String>,
}

#[derive(Debug)]
pub(super) struct PluginHealthRuntimeState {
    pub(super) state: PluginHealthState,
    pub(super) timed_out_active_calls: u64,
    pub(super) last_failure_call_id: u64,
    pub(super) last_error: Option<String>,
}

impl PluginHealthRuntimeState {
    pub(super) fn ready() -> Self {
        Self {
            state: PluginHealthState::Ready,
            timed_out_active_calls: 0,
            last_failure_call_id: 0,
            last_error: None,
        }
    }
}
