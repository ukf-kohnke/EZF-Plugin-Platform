//! Explicit runtime execution limits. Environment translation belongs to application adapters.
use std::time::Duration;

pub(super) const DEFAULT_PLUGIN_CALL_TIMEOUT_MS: u64 = 30_000;
pub(super) const DEFAULT_PLUGIN_UNLOAD_TIMEOUT_MS: u64 = 10_000;
pub(super) const DEFAULT_PLUGIN_LIFECYCLE_TIMEOUT_MS: u64 = 10_000;
// Patient-list load testing showed higher successful throughput at 16 workers. Keep admission
// and queue limits independent and bounded; validate this default against the target SQL Server.
pub(super) const DEFAULT_PLUGIN_WORKER_THREADS: usize = 16;
pub(super) const DEFAULT_PLUGIN_MAX_CONCURRENT_CALLS: usize = 32;
pub(super) const DEFAULT_PLUGIN_QUEUE_CAPACITY: usize = 24;

/// Runtime behavior limits for one plugin.
///
/// The current 16-worker default reflects the measured patient-list load profile; it is not a
/// universal optimum and should be validated against the target database. Admission and queue
/// limits remain bounded. Values are supplied by the application composition root; this runtime
/// type deliberately does not read process environment variables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PluginRuntimeConfig {
    pub call_timeout: Duration,
    /// Maximum time spent waiting for admitted calls to quiesce before lifecycle callbacks begin.
    pub unload_timeout: Duration,
    /// Maximum time to wait for one `on_unload` or `destroy` callback. A timeout requests
    /// cooperative cancellation; the callback remains active on the lifecycle worker until it
    /// returns and the runtime stays registered until that result is observed.
    pub lifecycle_timeout: Duration,
    /// Number of persistent host-side runtime threads available to dispatch this plugin's calls.
    /// For `ProcessExecutor`, these threads do not increase child-process parallelism: its current
    /// single-flight IPC protocol executes one top-level plugin operation at a time.
    pub worker_threads: usize,
    /// Maximum admitted calls across execution, queueing, and (in process mode) the single-flight
    /// IPC gate. This is an admission bound, not necessarily the number of parallel plugin calls.
    pub max_concurrent_calls: usize,
    /// Maximum number of jobs waiting for a worker. A full queue rejects new work with overload.
    pub queue_capacity: usize,
}

impl Default for PluginRuntimeConfig {
    fn default() -> Self {
        Self {
            call_timeout: Duration::from_millis(DEFAULT_PLUGIN_CALL_TIMEOUT_MS),
            unload_timeout: Duration::from_millis(DEFAULT_PLUGIN_UNLOAD_TIMEOUT_MS),
            lifecycle_timeout: Duration::from_millis(DEFAULT_PLUGIN_LIFECYCLE_TIMEOUT_MS),
            worker_threads: DEFAULT_PLUGIN_WORKER_THREADS,
            max_concurrent_calls: DEFAULT_PLUGIN_MAX_CONCURRENT_CALLS,
            queue_capacity: DEFAULT_PLUGIN_QUEUE_CAPACITY,
        }
    }
}
