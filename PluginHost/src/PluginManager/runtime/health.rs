//! Public plugin health snapshots and internal health state.
/// Identifies how plugin code is hosted by the current runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginExecutorKind {
    InProcess,
    Process,
}

impl PluginExecutorKind {
    /// Stable, machine-readable value suitable for logs and metrics labels.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InProcess => "in_process",
            Self::Process => "process",
        }
    }

    /// Maximum plugin-operation parallelism supported by the selected executor.
    ///
    /// The in-process executor can use the runtime worker pool, bounded by admission. The current
    /// process executor uses one worker process and a single-flight IPC gate, so its effective
    /// plugin-operation parallelism is one regardless of the host-side worker/admission settings.
    pub const fn max_parallel_plugin_calls(
        self,
        worker_threads: usize,
        max_concurrent_calls: usize
    ) -> usize {
        match self {
            Self::InProcess => {
                let workers = if worker_threads == 0 { 1 } else { worker_threads };
                let admitted = if max_concurrent_calls == 0 { 1 } else { max_concurrent_calls };
                if workers < admitted {
                    workers
                } else {
                    admitted
                }
            }
            Self::Process => 1,
        }
    }
}

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
    /// Concrete execution backend used for plugin code.
    pub executor: PluginExecutorKind,
    /// Process ID of the live child worker for the process executor; `None` for in-process
    /// execution and when the process worker has already exited.
    pub worker_pid: Option<u32>,
    /// Configured number of persistent host-side runtime worker threads for this plugin.
    /// For process execution these threads dispatch calls to one child worker; they do not imply
    /// that the child can execute the same number of plugin operations concurrently.
    pub worker_threads: usize,
    /// Maximum number of admitted calls, including calls waiting in the queue or at the process
    /// executor's single-flight IPC gate. This is not the effective plugin parallelism.
    pub max_concurrent_calls: usize,
    /// Maximum number of plugin operations that this executor can execute at once. This is
    /// `min(worker_threads, max_concurrent_calls)` for in-process execution and `1` for the current
    /// single-worker process executor.
    pub max_parallel_plugin_calls: usize,
    /// Configured capacity of the worker queue.
    pub queue_capacity: usize,
    /// Jobs currently waiting in the bounded runtime worker queue.
    pub queued_jobs: usize,
    /// Jobs occupying host-side runtime worker threads. In process mode, this can include calls
    /// waiting for the single-flight IPC gate rather than executing plugin code in the child.
    pub running_workers: usize,
    /// Calls admitted by the runtime that have not yet completed as plugin operations. In process
    /// mode this can include calls waiting for the child worker's single-flight IPC gate.
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
