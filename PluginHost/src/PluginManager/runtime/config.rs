//! Runtime configuration and environment parsing.
use std::time::Duration;

pub(super) const DEFAULT_PLUGIN_CALL_TIMEOUT_MS: u64 = 30_000;
pub(super) const DEFAULT_PLUGIN_UNLOAD_TIMEOUT_MS: u64 = 10_000;
pub(super) const DEFAULT_PLUGIN_LIFECYCLE_TIMEOUT_MS: u64 = 10_000;
// Patient-list load testing showed higher successful throughput at 16 workers. Keep admission
// and queue limits independent and bounded; validate this default against the target SQL Server.
pub(super) const DEFAULT_PLUGIN_WORKER_THREADS: usize = 16;
pub(super) const DEFAULT_PLUGIN_MAX_CONCURRENT_CALLS: usize = 32;
pub(super) const DEFAULT_PLUGIN_QUEUE_CAPACITY: usize = 24;
const MIN_PLUGIN_TIMEOUT_MS: u64 = 1;
const MAX_PLUGIN_TIMEOUT_MS: u64 = 10 * 60 * 1_000;
const MAX_PLUGIN_WORKER_THREADS: usize = 128;
const MAX_PLUGIN_CONCURRENT_CALLS: usize = 4_096;
const MAX_PLUGIN_QUEUE_CAPACITY: usize = 4_096;

/// Runtime behavior limits for one plugin.
///
/// The current 16-worker default reflects the measured patient-list load profile; it is not a
/// universal optimum and should be validated against the target database. Admission and queue
/// limits remain bounded. Production deployments may
/// construct one from `from_environment()` using `EZF_PLUGIN_CALL_TIMEOUT_MS`,
/// `EZF_PLUGIN_UNLOAD_TIMEOUT_MS`, `EZF_PLUGIN_LIFECYCLE_TIMEOUT_MS`,
/// `EZF_PLUGIN_WORKER_THREADS`, `EZF_PLUGIN_MAX_CONCURRENT_CALLS`, and `EZF_PLUGIN_QUEUE_CAPACITY`.
/// Explicitly configured PluginHost instances retain the supplied values rather than re-reading
/// the process environment during plugin loading.
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

impl PluginRuntimeConfig {
    pub fn from_environment() -> Self {
        let defaults = Self::default();
        Self {
            call_timeout: env_duration("EZF_PLUGIN_CALL_TIMEOUT_MS", defaults.call_timeout),
            unload_timeout: env_duration("EZF_PLUGIN_UNLOAD_TIMEOUT_MS", defaults.unload_timeout),
            lifecycle_timeout: env_duration(
                "EZF_PLUGIN_LIFECYCLE_TIMEOUT_MS",
                defaults.lifecycle_timeout
            ),
            worker_threads: env_usize(
                "EZF_PLUGIN_WORKER_THREADS",
                defaults.worker_threads,
                MAX_PLUGIN_WORKER_THREADS
            ),
            max_concurrent_calls: env_usize(
                "EZF_PLUGIN_MAX_CONCURRENT_CALLS",
                defaults.max_concurrent_calls,
                MAX_PLUGIN_CONCURRENT_CALLS
            ),
            queue_capacity: env_usize(
                "EZF_PLUGIN_QUEUE_CAPACITY",
                defaults.queue_capacity,
                MAX_PLUGIN_QUEUE_CAPACITY
            ),
        }
    }
}

fn env_duration(name: &str, default: Duration) -> Duration {
    let Ok(raw) = std::env::var(name) else {
        return default;
    };
    let Ok(ms) = raw.trim().parse::<u64>() else {
        return default;
    };
    Duration::from_millis(ms.clamp(MIN_PLUGIN_TIMEOUT_MS, MAX_PLUGIN_TIMEOUT_MS))
}

fn env_usize(name: &str, default: usize, maximum: usize) -> usize {
    let Ok(raw) = std::env::var(name) else {
        return default;
    };
    let Ok(value) = raw.trim().parse::<usize>() else {
        return default;
    };
    value.clamp(1, maximum)
}
