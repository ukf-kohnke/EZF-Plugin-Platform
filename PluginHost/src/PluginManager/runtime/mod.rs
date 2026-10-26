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

use libloading::Library;
use PluginAbi::{
    AbiBuffer,
    PluginCancellationV2,
    PluginInstanceV2,
    PluginVTableV2,
    MAX_ABI_BUFFER_BYTES,
    PLUGIN_ABI_VERSION_V2,
    PLUGIN_STATUS_OK,
};
use std::{
    sync::{ Arc, Mutex, RwLock, RwLockReadGuard, atomic::{ AtomicBool, AtomicU64, Ordering } },
};
use self::lifecycle_executor::{
    PendingPluginLifecycle,
    PluginLifecycleExecutor,
    PluginLifecycleOperation,
};
use self::worker_pool::{ PluginJob, PluginWorkerPool, PluginWorkerSubmitError };
pub use self::config::PluginRuntimeConfig;
pub use self::health::{ PluginHealthSnapshot, PluginHealthState };
use self::health::PluginHealthRuntimeState;
use self::calls::{
    PluginCallAccounting,
    PluginCallFailure,
    PluginCallGate,
    PluginCallTracker,
    PluginCancellationState,
};

fn never_cancelled_abi() -> PluginCancellationV2 {
    PluginCancellationV2 {
        context: std::ptr::null(),
        is_cancelled: never_cancelled,
    }
}

unsafe extern "C" fn never_cancelled(_context: *const std::ffi::c_void) -> bool {
    false
}

/// Runtime state of one loaded plugin.
///
/// The dynamic library and opaque plugin instance remain together. Calls are admitted through a
/// bounded gate and executed by persistent worker threads, so request traffic does not create a
/// new OS thread per plugin call. Each worker invocation holds a shared read lock, allowing
/// independent calls to execute concurrently. Unload first waits for all admitted calls to drain,
/// then invokes `on_unload` and `destroy` on a dedicated single lifecycle worker. Lifecycle callback
/// timeouts request cooperative cancellation; the runtime stays registered and the next unload
/// attempt observes the actual callback result before progressing.
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
    instance: Option<*mut PluginInstanceV2>,
    #[allow(dead_code)]
    library: Library,
}

// Raw instance pointers are only dereferenced while the PluginRuntime state is protected by the
// RwLock. Plugin implementations are Send + Sync, and the ABI exposes no Rust references.
unsafe impl Send for PluginRuntimeState {}
unsafe impl Sync for PluginRuntimeState {}
unsafe impl Send for PluginRuntime {}
unsafe impl Sync for PluginRuntime {}

impl PluginRuntime {
    fn is_valid_buffer(buffer: AbiBuffer) -> bool {
        if buffer.ptr.is_null() {
            return buffer.len == 0 && buffer.capacity == 0;
        }

        buffer.len > 0 &&
            buffer.len <= MAX_ABI_BUFFER_BYTES &&
            buffer.capacity >= buffer.len &&
            buffer.capacity <= MAX_ABI_BUFFER_BYTES
    }

    pub(super) fn new(
        instance: *mut PluginInstanceV2,
        library: Library,
        config: PluginRuntimeConfig
    ) -> Result<Arc<Self>, String> {
        if instance.is_null() {
            return Err("plugin constructor returned a null instance".to_owned());
        }

        let instance_ref = unsafe { &*instance };
        if
            instance_ref.abi_version != PLUGIN_ABI_VERSION_V2 ||
            instance_ref.struct_size < (std::mem::size_of::<PluginInstanceV2>() as u32) ||
            instance_ref.vtable.is_null() ||
            instance_ref.context.is_null()
        {
            return Err("plugin instance reported an incompatible ABI layout".to_owned());
        }

        let vtable = unsafe { &*instance_ref.vtable };
        if !vtable.is_compatible() {
            return Err("plugin vtable reported an incompatible ABI layout".to_owned());
        }

        let pointers = [
            vtable.name as usize,
            vtable.manifest as usize,
            vtable.on_load as usize,
            vtable.is_loaded as usize,
            vtable.handle_endpoint as usize,
            vtable.execute_database as usize,
            vtable.on_session_closed as usize,
            vtable.on_unload as usize,
            vtable.free_buffer as usize,
            vtable.destroy as usize,
        ];
        if pointers.iter().any(|pointer| *pointer == 0) {
            return Err("plugin vtable contains a null function pointer".to_owned());
        }

        let worker_pool = PluginWorkerPool::new(
            config.worker_threads,
            config.queue_capacity
        ).map_err(|error| {
            let _ = unsafe { (vtable.destroy)(instance, never_cancelled_abi()) };
            error
        })?;
        let lifecycle_executor = PluginLifecycleExecutor::new().map_err(|error| {
            let _ = unsafe { (vtable.destroy)(instance, never_cancelled_abi()) };
            error
        })?;

        Ok(
            Arc::new(Self {
                state: RwLock::new(PluginRuntimeState {
                    instance: Some(instance),
                    library,
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

    fn read_state(&self) -> RwLockReadGuard<'_, PluginRuntimeState> {
        self.state.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn with_instance<R, F>(&self, func: F) -> Result<R, String>
        where F: FnOnce(*mut std::ffi::c_void, &PluginVTableV2) -> R
    {
        let state = self.read_state();
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

        let instance = state.instance.ok_or_else(|| "plugin runtime is unloaded".to_owned())?;
        let instance_ref = unsafe { &*instance };
        if instance_ref.vtable.is_null() || instance_ref.context.is_null() {
            return Err("plugin runtime contains invalid ABI pointers".to_owned());
        }
        let vtable = unsafe { &*instance_ref.vtable };
        if !vtable.is_compatible() {
            return Err("plugin runtime contains an incompatible ABI vtable".to_owned());
        }
        Ok(func(instance_ref.context, vtable))
    }

    /// Executes a raw ABI buffer-returning call. Lifecycle, timeout, panic isolation, accounting,
    /// and admission control are provided by `call_with_timeout`; this helper must only be invoked
    /// from inside that guarded execution path.
    fn call_buffer<F>(
        &self,
        cancellation: PluginCancellationV2,
        select_call: F
    ) -> Result<Vec<u8>, String>
        where
            F: FnOnce(
                &PluginVTableV2
            ) -> unsafe extern "C" fn(
                *mut std::ffi::c_void,
                PluginCancellationV2,
                *mut AbiBuffer
            ) -> i32
    {
        self.with_instance(|context, vtable| {
            let mut response = AbiBuffer::EMPTY;
            let status = unsafe { select_call(vtable)(context, cancellation, &mut response) };
            if status != PLUGIN_STATUS_OK {
                if Self::is_valid_buffer(response) && !response.ptr.is_null() {
                    unsafe {
                        (vtable.free_buffer)(response);
                    }
                }
                return Err(format!("plugin ABI call failed with status {status}"));
            }

            if !Self::is_valid_buffer(response) {
                return Err("plugin returned an invalid response buffer".to_owned());
            }

            let bytes = unsafe { response.to_vec() };
            // Release through the plugin's allocator regardless of whether conversion succeeded.
            unsafe {
                (vtable.free_buffer)(response);
            }
            bytes.ok_or_else(|| "plugin returned an invalid response buffer".to_owned())
        })?
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
        let mut health = self.health.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if timed_out {
            health.timed_out_active_calls = health.timed_out_active_calls.saturating_sub(1);
        }
        health.last_failure_call_id = health.last_failure_call_id.max(call_id);
        if matches!(health.state, PluginHealthState::Ready | PluginHealthState::Degraded) {
            health.state = PluginHealthState::Degraded;
            health.last_error = Some(message);
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
        let health = self.health.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        PluginHealthSnapshot {
            name,
            state: health.state,
            worker_threads: self.config.worker_threads,
            max_concurrent_calls: self.config.max_concurrent_calls,
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
            pending.cancellation.local.store(true, Ordering::Release);
        }
        self.worker_pool.shutdown();
        self.lifecycle_executor.shutdown();
        let mut state = self.state.write().unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(instance) = state.instance.take() else {
            return;
        };

        let instance_ref = unsafe { &*instance };
        if instance_ref.vtable.is_null() || instance_ref.context.is_null() {
            return;
        }
        let vtable = unsafe { &*instance_ref.vtable };
        let _ = std::panic::catch_unwind(
            std::panic::AssertUnwindSafe(|| unsafe {
                let cancellation = never_cancelled_abi();
                let _ = (vtable.on_unload)(instance_ref.context, cancellation);
                let _ = (vtable.destroy)(instance, cancellation);
            })
        );
    }
}

#[cfg(test)]
mod tests {
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
        let parent = PluginCallTracker::new(None);
        let child = PluginCallTracker::new(Some(parent.abi_cancellation()));

        assert!(!(unsafe { parent.abi_cancellation().is_cancelled() }));
        assert!(!(unsafe { child.abi_cancellation().is_cancelled() }));

        assert!(parent.mark_timed_out(|| {}));
        assert!(unsafe { parent.abi_cancellation().is_cancelled() });
        assert!(unsafe { child.abi_cancellation().is_cancelled() });
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
