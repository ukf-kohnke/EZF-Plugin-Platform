//! Per-plugin runtime state and shared invariants.
//! Calls, plugin operations, lifecycle transitions, scheduling, configuration, and health are split into child modules.
mod worker_pool;
mod lifecycle_executor;
mod execution;
mod operations;
mod lifecycle;
mod config;
mod health;
mod calls;

use super::executor::{
    PluginCallContext,
    PluginCancellationSource,
    PluginExecutor,
    PluginLifecycleStatus,
};
use std::sync::{ Arc, Mutex, RwLock, atomic::{ AtomicBool, AtomicU64, Ordering } };
use std::time::Instant;
use self::lifecycle_executor::{
    PendingPluginLifecycle,
    PluginLifecycleExecutor,
    PluginLifecycleOperation,
};
use self::worker_pool::{ PluginJob, PluginWorkerPool, PluginWorkerSubmitError };
pub use self::config::PluginRuntimeConfig;
pub use self::health::{ PluginExecutorKind, PluginHealthSnapshot, PluginHealthState };
use self::health::PluginHealthRuntimeState;
use self::calls::{ PluginCallAccounting, PluginCallFailure, PluginCallGate, PluginCallTracker };

/// Result of a runtime unload attempt. `ForcedTermination` means the worker process has been
/// confirmed terminated/reaped and the registry entry can be removed, but plugin cleanup did not
/// finish through the graceful lifecycle handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PluginUnloadOutcome {
    Unloaded,
    ForcedTermination,
    Failed,
}

impl PluginUnloadOutcome {
    pub(crate) fn is_graceful(self) -> bool {
        self == Self::Unloaded
    }

    pub(crate) fn completed(self) -> bool {
        matches!(self, Self::Unloaded | Self::ForcedTermination)
    }
}

fn cleanup_call_context(operation_id: u64, timeout: std::time::Duration) -> PluginCallContext {
    PluginCallContext::new(
        operation_id,
        Instant::now() + timeout,
        PluginCancellationSource::new(None).token()
    )
}

/// Runtime state of one loaded plugin.
///
/// The runtime owns scheduling, deadlines, health, admission, and lifecycle transitions. Plugin
/// code is accessed only through `PluginExecutor`; no dynamic-library handles or ABI function
/// pointers are exposed to the runtime core. Request work runs on a bounded persistent worker pool,
/// while lifecycle operations use the dedicated lifecycle worker and preserve timeout/retry rules.
pub(crate) struct PluginRuntime {
    state: RwLock<PluginRuntimeState>,
    unloading: AtomicBool,
    // Once on_unload has succeeded, retries must resume at destroy rather than re-entering it.
    on_unload_completed: AtomicBool,
    config: PluginRuntimeConfig,
    gate: Arc<PluginCallGate>,
    worker_pool: PluginWorkerPool,
    lifecycle_executor: PluginLifecycleExecutor,
    pending_lifecycle: Mutex<Option<PendingPluginLifecycle>>,
    health: Mutex<PluginHealthRuntimeState>,
    next_call_id: AtomicU64,
    total_calls: AtomicU64,
    completed_calls: AtomicU64,
    failed_calls: AtomicU64,
    timed_out_calls: AtomicU64,
    rejected_calls: AtomicU64,
    name: Mutex<Option<String>>,
}

struct PluginRuntimeState {
    executor: Option<Arc<dyn PluginExecutor>>,
    // Keep the executor (and therefore its library image) alive until the final Runtime Arc drops,
    // matching the previous behavior where `Library` outlived `destroy()`.
    destroyed: bool,
}

impl PluginRuntime {
    pub(super) fn new(
        executor: Arc<dyn PluginExecutor>,
        config: PluginRuntimeConfig
    ) -> Result<Arc<Self>, String> {
        executor.validate()?;

        let worker_pool = PluginWorkerPool::new(
            config.worker_threads,
            config.queue_capacity
        ).map_err(|error| {
            let context = cleanup_call_context(0, config.lifecycle_timeout);
            let _ = executor.destroy(&context);
            error
        })?;
        let lifecycle_executor = PluginLifecycleExecutor::new().map_err(|error| {
            let context = cleanup_call_context(0, config.lifecycle_timeout);
            let _ = executor.destroy(&context);
            error
        })?;

        Ok(
            Arc::new(Self {
                state: RwLock::new(PluginRuntimeState {
                    executor: Some(executor),
                    destroyed: false,
                }),
                unloading: AtomicBool::new(false),
                on_unload_completed: AtomicBool::new(false),
                gate: Arc::new(PluginCallGate::new(config.max_concurrent_calls)),
                worker_pool,
                lifecycle_executor,
                pending_lifecycle: Mutex::new(None),
                config,
                health: Mutex::new(PluginHealthRuntimeState::ready()),
                next_call_id: AtomicU64::new(0),
                total_calls: AtomicU64::new(0),
                completed_calls: AtomicU64::new(0),
                failed_calls: AtomicU64::new(0),
                timed_out_calls: AtomicU64::new(0),
                rejected_calls: AtomicU64::new(0),
                name: Mutex::new(None),
            })
        )
    }

    /// Executes one logical operation while retaining the read-side lifecycle guard for the full
    /// operation. This prevents teardown from releasing the executor during an in-flight call.
    fn with_executor<R, F>(&self, func: F) -> Result<R, String>
        where F: FnOnce(&dyn PluginExecutor) -> Result<R, String>
    {
        let state = self.state.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        // `destroy()` invalidates the executor's opaque plugin instance. Check the authoritative
        // state before health so even a poisoned/temporarily inconsistent health lock cannot let a
        // post-destroy call reach the executor.
        if state.destroyed {
            return Err("plugin runtime is unloaded".to_owned());
        }
        if self.unloading.load(Ordering::Acquire) {
            return Err("plugin runtime is unloading".to_owned());
        }

        let health = self.health.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        match health.state {
            PluginHealthState::Ready | PluginHealthState::Degraded => {}
            PluginHealthState::Loading => {
                return Err("plugin runtime is still loading".to_owned());
            }
            PluginHealthState::Stopping => {
                return Err("plugin runtime is stopping".to_owned());
            }
            PluginHealthState::Failed => {
                return Err("plugin runtime is failed".to_owned());
            }
            PluginHealthState::Unloaded => {
                return Err("plugin runtime is unloaded".to_owned());
            }
        }
        drop(health);

        let executor = state.executor
            .as_deref()
            .ok_or_else(|| "plugin runtime is unloaded".to_owned())?;
        func(executor)
    }

    fn set_health(&self, state: PluginHealthState) {
        let mut health = self.health.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        health.state = state;
    }

    fn set_health_error(&self, state: PluginHealthState, message: impl Into<String>) {
        let mut health = self.health.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        health.state = state;
        health.last_error = Some(message.into());
    }

    fn record_success(&self, call_id: u64, timed_out: bool) {
        let mut health = self.health.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if timed_out {
            health.timed_out_active_calls = health.timed_out_active_calls.saturating_sub(1);
        }

        if
            matches!(health.state, PluginHealthState::Ready | PluginHealthState::Degraded) &&
            health.timed_out_active_calls == 0 &&
            call_id >= health.last_failure_call_id
        {
            health.state = PluginHealthState::Ready;
            health.last_error = None;
        }
        drop(health);
        self.completed_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an actual plugin-operation failure. This is deliberately separate from a
    /// request timeout: the latter only means the caller stopped waiting; the plugin operation
    /// may still finish successfully afterwards.
    fn record_failure(&self, call_id: u64, message: impl Into<String>, timed_out: bool) {
        let message = message.into();
        // A process-backed executor can fail because the worker died, not merely because the
        // plugin returned a domain error. Revalidate before taking the health lock so a dead worker
        // transitions the runtime to Failed instead of remaining Degraded indefinitely.
        let executor_error = {
            let state = self.state.read().unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.destroyed {
                None
            } else {
                state.executor.as_deref().and_then(|executor| executor.validate().err())
            }
        };
        let mut health = self.health.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if timed_out {
            health.timed_out_active_calls = health.timed_out_active_calls.saturating_sub(1);
        }
        health.last_failure_call_id = health.last_failure_call_id.max(call_id);
        if matches!(health.state, PluginHealthState::Ready | PluginHealthState::Degraded) {
            if let Some(executor_error) = executor_error {
                health.state = PluginHealthState::Failed;
                health.last_error = Some(
                    format!("{message}; executor unavailable: {executor_error}")
                );
            } else {
                health.state = PluginHealthState::Degraded;
                health.last_error = Some(message);
            }
        }
        drop(health);
        self.completed_calls.fetch_add(1, Ordering::Relaxed);
        self.failed_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn record_submission_failure(&self, message: impl Into<String>) {
        let mut health = self.health.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if matches!(health.state, PluginHealthState::Ready | PluginHealthState::Degraded) {
            health.state = PluginHealthState::Degraded;
            health.last_error = Some(message.into());
        }
        drop(health);
        self.failed_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn record_request_timeout(&self, message: impl Into<String>) {
        let mut health = self.health.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        health.timed_out_active_calls = health.timed_out_active_calls.saturating_add(1);
        if matches!(health.state, PluginHealthState::Ready | PluginHealthState::Degraded) {
            health.state = PluginHealthState::Degraded;
            health.last_error = Some(message.into());
        }
        drop(health);
        self.timed_out_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> PluginHealthSnapshot {
        let name = self.name
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .unwrap_or_else(|| "<initializing>".to_owned());

        // Readiness/health probes must also notice a worker that exited while idle, before a new
        // endpoint happens to exercise the executor. `validate()` is non-blocking for both
        // implementations (the process executor uses Child::try_wait).
        let (executor_kind, worker_pid, executor_error) = {
            let state = self.state.read().unwrap_or_else(|poisoned| poisoned.into_inner());
            match state.executor.as_deref() {
                Some(executor) => {
                    let error = if state.destroyed { None } else { executor.validate().err() };
                    (executor.kind(), executor.worker_pid(), error)
                }
                None => (PluginExecutorKind::InProcess, None, None),
            }
        };
        let mut health = self.health.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(error) = executor_error {
            if matches!(health.state, PluginHealthState::Ready | PluginHealthState::Degraded) {
                health.state = PluginHealthState::Failed;
                health.last_error = Some(format!("executor unavailable: {error}"));
            }
        }
        PluginHealthSnapshot {
            name,
            state: health.state,
            executor: executor_kind,
            worker_pid,
            worker_threads: self.config.worker_threads,
            max_concurrent_calls: self.config.max_concurrent_calls,
            max_parallel_plugin_calls: executor_kind.max_parallel_plugin_calls(
                self.config.worker_threads,
                self.config.max_concurrent_calls
            ),
            queue_capacity: self.worker_pool.queue_capacity,
            queued_jobs: self.worker_pool.queued_jobs.load(Ordering::Relaxed),
            running_workers: self.worker_pool.running_workers.load(Ordering::Relaxed),
            active_calls: self.gate.active_calls(),
            timed_out_active_calls: health.timed_out_active_calls,
            completed_calls: self.completed_calls.load(Ordering::Relaxed),
            failed_calls: self.failed_calls.load(Ordering::Relaxed),
            timed_out_calls: self.timed_out_calls.load(Ordering::Relaxed),
            total_calls: self.total_calls.load(Ordering::Relaxed),
            rejected_calls: self.rejected_calls.load(Ordering::Relaxed),
            last_error: health.last_error.clone(),
        }
    }

    fn set_name(&self, name: String) {
        let mut value = self.name.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        *value = Some(name);
    }
}

impl Drop for PluginRuntime {
    fn drop(&mut self) {
        self.unloading.store(true, Ordering::Release);
        self.gate.cancel_all();
        if
            let Some(pending) = self.pending_lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
        {
            pending.cancellation.cancel();
        }
        self.worker_pool.shutdown();
        self.lifecycle_executor.shutdown();

        let mut state = self.state.write().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(executor) = state.executor.take() {
            if !state.destroyed {
                let _ = std::panic::catch_unwind(
                    std::panic::AssertUnwindSafe(|| {
                        executor.emergency_shutdown();
                    })
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Factory::PluginExecutorKind;
    use crate::PluginManager::runtime::config::{
        DEFAULT_PLUGIN_MAX_CONCURRENT_CALLS,
        DEFAULT_PLUGIN_QUEUE_CAPACITY,
        DEFAULT_PLUGIN_WORKER_THREADS,
    };

    use super::{
        PluginCallTracker,
        PluginRuntimeConfig,
        PluginWorkerPool,
        PluginWorkerSubmitError,
    };
    use PluginAbi::PluginCancellationV2;
    use super::super::executor::PluginCancellationToken;
    use std::sync::{ Arc, Condvar, Mutex, mpsc };
    use std::time::Duration;

    #[test]
    fn timed_out_call_lifecycle_is_closed_by_the_worker() {
        let tracker = PluginCallTracker::new(None);
        let mut timeout_bookkeeping = false;

        assert!(tracker.mark_timed_out(|| timeout_bookkeeping = true));
        assert!(timeout_bookkeeping);
        assert!(tracker.complete());
    }

    #[test]
    fn completed_call_wins_the_timeout_race() {
        let tracker = PluginCallTracker::new(None);

        assert!(!tracker.complete());
        assert!(!tracker.mark_timed_out(|| panic!("completed call must not be timed out")));
    }

    #[test]
    fn cancellation_token_reflects_local_and_parent_cancellation() {
        unsafe extern "C" fn is_cancelled(context: *const std::ffi::c_void) -> bool {
            if context.is_null() {
                return true;
            }
            let token = unsafe { &*(context as *const PluginCancellationToken) };
            token.is_cancelled()
        }

        let parent = PluginCallTracker::new(None);
        let parent_token = parent.cancellation_token();
        let parent_abi = PluginCancellationV2 {
            context: (&parent_token as *const PluginCancellationToken).cast(),
            is_cancelled,
        };
        let child = PluginCallTracker::new(Some(parent_abi));
        let child_token = child.cancellation_token();

        assert!(!parent_token.is_cancelled());
        assert!(!child_token.is_cancelled());

        assert!(parent.mark_timed_out(|| {}));
        assert!(parent_token.is_cancelled());
        assert!(child_token.is_cancelled());
    }

    #[test]
    fn effective_plugin_parallelism_depends_on_executor_kind() {
        assert_eq!(PluginExecutorKind::InProcess.max_parallel_plugin_calls(16, 32), 16);
        assert_eq!(PluginExecutorKind::InProcess.max_parallel_plugin_calls(8, 3), 3);
        assert_eq!(PluginExecutorKind::Process.max_parallel_plugin_calls(16, 32), 1);
        assert_eq!(PluginExecutorKind::Process.max_parallel_plugin_calls(1, 1), 1);
    }

    #[test]
    fn runtime_defaults_separate_workers_admission_and_queue_capacity() {
        let config = PluginRuntimeConfig::default();
        assert_eq!(config.worker_threads, DEFAULT_PLUGIN_WORKER_THREADS);
        assert_eq!(config.max_concurrent_calls, DEFAULT_PLUGIN_MAX_CONCURRENT_CALLS);
        assert_eq!(config.queue_capacity, DEFAULT_PLUGIN_QUEUE_CAPACITY);
    }

    #[test]
    fn worker_count_and_queue_capacity_are_independent_and_queue_is_bounded() {
        let pool = PluginWorkerPool::new(2, 3).expect("two worker threads must start");
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));

        let blocking_job = |
            started_tx: mpsc::Sender<()>,
            release: Arc<(Mutex<bool>, Condvar)>
        | -> super::PluginJob {
            Box::new(move || {
                started_tx.send(()).expect("test receiver remains connected");
                let (lock, ready) = &*release;
                let mut released = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                while !*released {
                    released = ready
                        .wait(released)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            })
        };

        // Both worker threads enter a blocking job and remain occupied while the test fills the
        // queue. Receiving two starts proves worker_count=2 independently of queue_capacity=3.
        for _ in 0..2 {
            pool.submit(blocking_job(started_tx.clone(), Arc::clone(&release))).unwrap_or_else(|_|
                panic!("running jobs must be accepted")
            );
        }
        started_rx.recv_timeout(Duration::from_secs(2)).expect("first worker must start");
        started_rx.recv_timeout(Duration::from_secs(2)).expect("second worker must start");

        // While both workers are occupied, exactly three additional jobs can wait in the queue.
        for _ in 0..3 {
            pool.submit(blocking_job(started_tx.clone(), Arc::clone(&release))).unwrap_or_else(|_|
                panic!("queue must accept jobs up to its independent capacity")
            );
        }
        match pool.submit(blocking_job(started_tx.clone(), Arc::clone(&release))) {
            Err(PluginWorkerSubmitError::QueueFull(job)) => drop(job),
            Err(PluginWorkerSubmitError::Closed(_)) => panic!("worker pool must still be open"),
            Ok(()) =>
                panic!("a sixth job must be rejected with two workers and queue capacity three"),
        }

        let (lock, ready) = &*release;
        *lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        ready.notify_all();

        for _ in 0..3 {
            started_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("queued jobs must drain after release");
        }
        drop(pool); // joins both persistent workers after their bounded queue drains.
    }
}
