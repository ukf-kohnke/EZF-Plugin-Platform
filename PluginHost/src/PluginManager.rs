use PluginContracts::{ ApiContract::{ PluginCatalog, PluginManifest }, IPlugin::IPlugin };
use super::Factory::PluginManagerError;
use glob::{ glob, GlobError };
use libloading::{ Library, Symbol };
use log::LevelFilter;
use serde::de::DeserializeOwned;
use PluginAbi::{
    AbiBuffer,
    AbiSlice,
    PluginCreateFnV1,
    PluginDestroyFnV1,
    PluginEndpointFnV1,
    PluginLoadFnV1,
    PluginHostServicesV1,
    PluginInstanceV1,
    PluginVTableV1,
    MAX_ABI_BUFFER_BYTES,
    PLUGIN_ABI_VERSION_V1,
    PLUGIN_CONSTRUCTOR_SYMBOL_V1,
    PLUGIN_STATUS_NOT_INITIALIZED,
    PLUGIN_STATUS_OK,
};
use std::{
    collections::HashMap,
    path::{ Path, PathBuf },
    sync::{
        mpsc,
        Arc,
        Mutex,
        RwLock,
        RwLockReadGuard,
        atomic::{ AtomicBool, AtomicU64, Ordering },
        Condvar,
    },
    thread,
    time::{ Duration, Instant },
};

const PLUGIN_AREA_ID: i64 = 0x00ff_ffff;
const DEFAULT_PLUGIN_CALL_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_PLUGIN_UNLOAD_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_PLUGIN_LIFECYCLE_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_PLUGIN_MAX_CONCURRENT_CALLS: usize = 64;
const MIN_PLUGIN_TIMEOUT_MS: u64 = 1;
const MAX_PLUGIN_TIMEOUT_MS: u64 = 10 * 60 * 1_000;
const MAX_PLUGIN_CONCURRENT_CALLS: usize = 1_024;

/// Runtime behavior limits for one plugin.
///
/// The defaults are intentionally conservative enough for normal API/database work while still
/// bounding the amount of work a misbehaving plugin can consume. Production deployments may
/// override them globally through environment variables:
/// `EZF_PLUGIN_CALL_TIMEOUT_MS`, `EZF_PLUGIN_UNLOAD_TIMEOUT_MS`, `EZF_PLUGIN_LIFECYCLE_TIMEOUT_MS`, and
/// `EZF_PLUGIN_MAX_CONCURRENT_CALLS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PluginRuntimeConfig {
    pub call_timeout: Duration,
    /// Maximum time spent waiting for admitted calls to quiesce before lifecycle callbacks begin.
    pub unload_timeout: Duration,
    /// Maximum time to wait for one `on_unload` or `destroy` callback. A timeout never cancels
    /// native code; the callback continues on the dedicated lifecycle worker until it returns.
    pub lifecycle_timeout: Duration,
    pub max_concurrent_calls: usize,
}

impl Default for PluginRuntimeConfig {
    fn default() -> Self {
        Self {
            call_timeout: Duration::from_millis(DEFAULT_PLUGIN_CALL_TIMEOUT_MS),
            unload_timeout: Duration::from_millis(DEFAULT_PLUGIN_UNLOAD_TIMEOUT_MS),
            lifecycle_timeout: Duration::from_millis(DEFAULT_PLUGIN_LIFECYCLE_TIMEOUT_MS),
            max_concurrent_calls: DEFAULT_PLUGIN_MAX_CONCURRENT_CALLS,
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
            max_concurrent_calls: env_usize(
                "EZF_PLUGIN_MAX_CONCURRENT_CALLS",
                defaults.max_concurrent_calls
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

fn env_usize(name: &str, default: usize) -> usize {
    let Ok(raw) = std::env::var(name) else {
        return default;
    };
    let Ok(value) = raw.trim().parse::<usize>() else {
        return default;
    };
    value.clamp(1, MAX_PLUGIN_CONCURRENT_CALLS)
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
struct PluginHealthRuntimeState {
    state: PluginHealthState,
    timed_out_active_calls: u64,
    last_failure_call_id: u64,
    last_error: Option<String>,
}

impl PluginHealthRuntimeState {
    fn ready() -> Self {
        Self {
            state: PluginHealthState::Ready,
            timed_out_active_calls: 0,
            last_failure_call_id: 0,
            last_error: None,
        }
    }
}

struct PluginCallGate {
    state: Mutex<usize>,
    max_concurrent_calls: usize,
    idle: Condvar,
}

impl PluginCallGate {
    fn new(max_concurrent_calls: usize) -> Self {
        Self {
            state: Mutex::new(0),
            max_concurrent_calls: max_concurrent_calls.max(1),
            idle: Condvar::new(),
        }
    }

    /// Admission is deliberately non-blocking. A full plugin is considered overloaded instead
    /// of making a caller wait behind already admitted work. Request execution still has its own
    /// timeout after admission.
    fn try_acquire(self: &Arc<Self>) -> Result<PluginCallPermit, ()> {
        let mut active = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if *active >= self.max_concurrent_calls {
            return Err(());
        }
        *active += 1;
        Ok(PluginCallPermit { gate: Arc::clone(self) })
    }

    fn active_calls(&self) -> usize {
        *self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn wait_until_idle(&self, deadline: Instant) -> bool {
        let mut active = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        while *active != 0 {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (guard, timeout) = self.idle
                .wait_timeout(active, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            active = guard;
            if timeout.timed_out() && *active != 0 {
                return false;
            }
        }
        true
    }
}

struct PluginCallPermit {
    gate: Arc<PluginCallGate>,
}

impl Drop for PluginCallPermit {
    fn drop(&mut self) {
        let mut active = self.gate.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        *active = (*active).saturating_sub(1);
        if *active == 0 {
            self.gate.idle.notify_all();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PluginCallLifecycleState {
    Pending,
    TimedOut,
    Completed,
}

struct PluginCallTracker {
    state: Mutex<PluginCallLifecycleState>,
}

impl PluginCallTracker {
    fn new() -> Self {
        Self { state: Mutex::new(PluginCallLifecycleState::Pending) }
    }

    /// Marks the caller-visible timeout. The callback runs while the call is still protected by
    /// the lifecycle lock, so the worker cannot complete the same call between the state transition
    /// and the associated timeout bookkeeping. This makes the timeout/completion race deterministic.
    fn mark_timed_out<F>(&self, on_marked: F) -> bool where F: FnOnce() {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if *state != PluginCallLifecycleState::Pending {
            return false;
        }
        *state = PluginCallLifecycleState::TimedOut;
        on_marked();
        true
    }

    /// Completes the underlying plugin operation. Returns whether the caller had already timed
    /// out, allowing the worker to close the corresponding `timed_out_active_calls` lifecycle.
    fn complete(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let timed_out = *state == PluginCallLifecycleState::TimedOut;
        if *state != PluginCallLifecycleState::Completed {
            *state = PluginCallLifecycleState::Completed;
        }
        timed_out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PluginCallAccounting {
    /// A caller-visible plugin operation. These calls contribute to runtime health and call
    /// counters because they represent productive plugin traffic.
    Observed,
    /// Loader-internal plugin execution (currently `name` and `manifest`). These operations still
    /// use the full bounded/timeout/panic-isolated execution path, but must not pollute productive
    /// call counters because they happen before the plugin is registered.
    Internal,
}

#[derive(Debug)]
enum PluginCallFailure {
    Busy,
    Timeout(Duration),
    Failed(String),
}

impl std::fmt::Display for PluginCallFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => write!(f, "plugin call concurrency limit reached"),
            Self::Timeout(timeout) => write!(f, "plugin call timed out after {:?}", timeout),
            Self::Failed(message) => write!(f, "{message}"),
        }
    }
}

/// Runtime state of one loaded plugin.
///
/// The dynamic library and opaque plugin instance remain together. Calls are admitted through a
/// bounded gate and executed by persistent worker threads, so request traffic does not create a
/// new OS thread per plugin call. Each worker invocation holds a shared read lock, allowing
/// independent calls to execute concurrently. Unload first waits for all admitted calls to drain,
/// then invokes `on_unload` and `destroy` on a dedicated single lifecycle worker. Lifecycle callback
/// timeouts never forcefully cancel native code; the runtime stays registered and the next unload
/// attempt observes the actual callback result before progressing.
pub(crate) struct PluginRuntime {
    state: RwLock<PluginRuntimeState>,
    unloading: AtomicBool,
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
    instance: Option<*mut PluginInstanceV1>,
    #[allow(dead_code)]
    library: Library,
}

type PluginJob = Box<dyn FnOnce() + Send + 'static>;

struct PluginWorkerPool {
    sender: Mutex<Option<mpsc::SyncSender<PluginJob>>>,
    workers: Mutex<Option<Vec<thread::JoinHandle<()>>>>,
}

impl PluginWorkerPool {
    fn new(worker_count: usize) -> Result<Self, String> {
        let worker_count = worker_count.max(1);
        let (sender, receiver) = mpsc::sync_channel::<PluginJob>(worker_count);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(worker_count);

        for index in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            let thread_name = format!("ezf-plugin-worker-{index}");
            match
                thread::Builder
                    ::new()
                    .name(thread_name)
                    .spawn(move || {
                        loop {
                            let job = {
                                let receiver = receiver
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                receiver.recv()
                            };

                            match job {
                                Ok(job) => job(),
                                Err(_) => {
                                    break;
                                }
                            }
                        }
                    })
            {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    drop(sender);
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(format!("failed to spawn plugin worker {index}: {error}"));
                }
            }
        }

        Ok(Self {
            sender: Mutex::new(Some(sender)),
            workers: Mutex::new(Some(workers)),
        })
    }

    fn submit(&self, job: PluginJob) -> Result<(), PluginJob> {
        let sender = self.sender.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(sender) = sender.as_ref() else {
            return Err(job);
        };

        match sender.try_send(job) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(job)) | Err(mpsc::TrySendError::Disconnected(job)) =>
                Err(job),
        }
    }

    fn shutdown(&self) {
        let sender = self.sender
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        drop(sender);

        let workers = self.workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(workers) = workers else {
            return;
        };

        let current_thread_id = thread::current().id();
        for worker in workers {
            // A last Arc<PluginRuntime> can theoretically be released by the worker executing a
            // timed-out job. Never attempt to join the current worker thread from itself; dropping
            // that JoinHandle detaches it and it will exit as soon as the queue observes closure.
            if worker.thread().id() == current_thread_id {
                continue;
            }
            let _ = worker.join();
        }
    }
}

impl Drop for PluginWorkerPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PluginLifecycleOperation {
    OnUnload,
    Destroy,
}

struct PendingPluginLifecycle {
    operation: PluginLifecycleOperation,
    receiver: mpsc::Receiver<Result<i32, String>>,
}

type PluginLifecycleJob = Box<dyn (FnOnce() -> Result<i32, String>) + Send + 'static>;

/// Dedicated single-worker executor for lifecycle callbacks. Lifecycle operations do not consume
/// normal request-worker capacity. A timed-out callback is intentionally allowed to finish on this
/// persistent worker; the runtime remains registered and will only proceed to the next lifecycle
/// stage after the callback's actual result has been observed.
struct PluginLifecycleExecutor {
    sender: Mutex<Option<mpsc::SyncSender<PluginLifecycleJob>>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
}

impl PluginLifecycleExecutor {
    fn new() -> Result<Self, String> {
        let (sender, receiver) = mpsc::sync_channel::<PluginLifecycleJob>(1);
        let worker = thread::Builder
            ::new()
            .name("ezf-plugin-lifecycle-worker".to_owned())
            .spawn(move || {
                loop {
                    let job = match receiver.recv() {
                        Ok(job) => job,
                        Err(_) => {
                            break;
                        }
                    };
                    let _ = job();
                }
            })
            .map_err(|error| format!("failed to spawn plugin lifecycle worker: {error}"))?;

        Ok(Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(Some(worker)),
        })
    }

    fn submit(
        &self,
        job: PluginLifecycleJob
    ) -> Result<mpsc::Receiver<Result<i32, String>>, String> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let wrapped: PluginLifecycleJob = Box::new(move || {
            let result = std::panic
                ::catch_unwind(std::panic::AssertUnwindSafe(job))
                .unwrap_or_else(|_| Err("plugin lifecycle callback panicked".to_owned()));
            let _ = sender.send(result.clone());
            result
        });

        let sender = self.sender.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(sender) = sender.as_ref() else {
            return Err("plugin lifecycle worker is shut down".to_owned());
        };
        sender
            .try_send(wrapped)
            .map_err(|_| "plugin lifecycle worker is busy or disconnected".to_owned())?;
        Ok(receiver)
    }

    fn shutdown(&self) {
        let sender = self.sender
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        drop(sender);

        let worker = self.worker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(worker) = worker else {
            return;
        };
        if worker.thread().id() != thread::current().id() {
            let _ = worker.join();
        }
    }
}

impl Drop for PluginLifecycleExecutor {
    fn drop(&mut self) {
        self.shutdown();
    }
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

    fn new(
        instance: *mut PluginInstanceV1,
        library: Library,
        config: PluginRuntimeConfig
    ) -> Result<Arc<Self>, String> {
        if instance.is_null() {
            return Err("plugin constructor returned a null instance".to_owned());
        }

        let instance_ref = unsafe { &*instance };
        if
            instance_ref.abi_version != PLUGIN_ABI_VERSION_V1 ||
            instance_ref.struct_size < (std::mem::size_of::<PluginInstanceV1>() as u32) ||
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

        let worker_pool = PluginWorkerPool::new(config.max_concurrent_calls).map_err(|error| {
            let _ = unsafe { (vtable.destroy)(instance) };
            error
        })?;
        let lifecycle_executor = PluginLifecycleExecutor::new().map_err(|error| {
            let _ = unsafe { (vtable.destroy)(instance) };
            error
        })?;

        Ok(
            Arc::new(Self {
                state: RwLock::new(PluginRuntimeState {
                    instance: Some(instance),
                    library,
                }),
                unloading: AtomicBool::new(false),
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
        where F: FnOnce(*mut std::ffi::c_void, &PluginVTableV1) -> R
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
    fn call_buffer<F>(&self, select_call: F) -> Result<Vec<u8>, String>
        where
            F: FnOnce(
                &PluginVTableV1
            ) -> unsafe extern "C" fn(*mut std::ffi::c_void, *mut AbiBuffer) -> i32
    {
        self.with_instance(|context, vtable| {
            let mut response = AbiBuffer::EMPTY;
            let status = unsafe { select_call(vtable)(context, &mut response) };
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

            let bytes = (unsafe { response.to_vec() }).ok_or_else(||
                "plugin returned an invalid response buffer".to_owned()
            )?;
            unsafe {
                (vtable.free_buffer)(response);
            }
            Ok(bytes)
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

    fn snapshot(&self) -> PluginHealthSnapshot {
        let name = self.name
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .unwrap_or_else(|| "<initializing>".to_owned());
        let health = self.health.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        PluginHealthSnapshot {
            name,
            state: health.state,
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

    fn call_with_timeout<R, F>(
        self: &Arc<Self>,
        operation: &'static str,
        call: F
    )
        -> Result<R, PluginCallFailure>
        where R: Send + 'static, F: FnOnce(&PluginRuntime) -> Result<R, String> + Send + 'static
    {
        self.call_with_timeout_accounted(operation, PluginCallAccounting::Observed, call)
    }

    fn call_with_timeout_internal<R, F>(
        self: &Arc<Self>,
        operation: &'static str,
        call: F
    )
        -> Result<R, PluginCallFailure>
        where R: Send + 'static, F: FnOnce(&PluginRuntime) -> Result<R, String> + Send + 'static
    {
        self.call_with_timeout_accounted(operation, PluginCallAccounting::Internal, call)
    }

    fn call_with_timeout_accounted<R, F>(
        self: &Arc<Self>,
        operation: &'static str,
        accounting: PluginCallAccounting,
        call: F
    )
        -> Result<R, PluginCallFailure>
        where R: Send + 'static, F: FnOnce(&PluginRuntime) -> Result<R, String> + Send + 'static
    {
        let permit = match self.gate.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                self.rejected_calls.fetch_add(1, Ordering::Relaxed);
                Common::Logging::Logger::Log::Dump(
                    "{}",
                    Common::Logging::LogConstants::LOGGERDEF,
                    LevelFilter::Warn,
                    &[
                        &format!(
                            "Plugin operation '{}' rejected: concurrency limit {} reached",
                            operation,
                            self.config.max_concurrent_calls
                        ),
                    ]
                );
                return Err(PluginCallFailure::Busy);
            }
        };

        if self.unloading.load(Ordering::Acquire) {
            drop(permit);
            return Err(PluginCallFailure::Failed("plugin runtime is unloading".to_owned()));
        }

        let call_id = self.next_call_id.fetch_add(1, Ordering::Relaxed) + 1;
        if accounting == PluginCallAccounting::Observed {
            self.total_calls.fetch_add(1, Ordering::Relaxed);
        }
        let tracker = Arc::new(PluginCallTracker::new());
        let runtime = Arc::clone(self);
        let worker_tracker = Arc::clone(&tracker);
        let (sender, receiver) = mpsc::sync_channel(1);
        let job: PluginJob = Box::new(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| call(&runtime)));
            let result = match result {
                Ok(result) => result,
                Err(_) => Err(format!("plugin operation '{operation}' panicked")),
            };
            let was_timed_out = worker_tracker.complete();

            if accounting == PluginCallAccounting::Observed {
                match &result {
                    Ok(_) => runtime.record_success(call_id, was_timed_out),
                    Err(error) => runtime.record_failure(call_id, error.clone(), was_timed_out),
                }
            }

            if was_timed_out {
                let outcome = if result.is_ok() { "success" } else { "failure" };
                Common::Logging::Logger::Log::Dump(
                    "{}",
                    Common::Logging::LogConstants::LOGGERDEF,
                    LevelFilter::Info,
                    &[
                        &format!("Plugin operation '{}' completed after the caller timed out ({outcome})", operation),
                    ]
                );
            }

            // Release admission before publishing completion. Otherwise the caller can observe
            // the completed result and immediately submit a new call while the previous permit
            // is still held, producing a spurious `Busy` rejection.
            drop(permit);
            let _ = sender.send(result);
        });

        if self.worker_pool.submit(job).is_err() {
            let message = "plugin worker pool rejected an admitted call".to_owned();
            if accounting == PluginCallAccounting::Observed {
                self.record_submission_failure(message.clone());
            }
            return Err(PluginCallFailure::Failed(message));
        }

        match receiver.recv_timeout(self.config.call_timeout) {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(PluginCallFailure::Failed(error)),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let timed_out = if accounting == PluginCallAccounting::Observed {
                    tracker.mark_timed_out(|| {
                        self.record_request_timeout(
                            format!(
                                "plugin operation '{operation}' timed out after {:?}; underlying plugin operation remains active",
                                self.config.call_timeout
                            )
                        );
                    })
                } else {
                    tracker.mark_timed_out(|| {})
                };
                if timed_out {
                    let message = format!(
                        "plugin operation '{operation}' timed out after {:?}; underlying plugin operation remains active",
                        self.config.call_timeout
                    );
                    Common::Logging::Logger::Log::Dump(
                        "{}",
                        Common::Logging::LogConstants::LOGGERDEF,
                        LevelFilter::Warn,
                        &[message.as_str()]
                    );
                    Err(PluginCallFailure::Timeout(self.config.call_timeout))
                } else {
                    // The worker won the timeout/completion race. The result is already on the
                    // completion channel, so consume it instead of manufacturing a false timeout.
                    match receiver.recv() {
                        Ok(Ok(result)) => Ok(result),
                        Ok(Err(error)) => Err(PluginCallFailure::Failed(error)),
                        Err(_) => {
                            let message = format!(
                                "plugin operation '{operation}' worker disconnected"
                            );
                            Err(PluginCallFailure::Failed(message))
                        }
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let message = format!("plugin operation '{operation}' worker disconnected");
                Err(PluginCallFailure::Failed(message))
            }
        }
    }

    fn call_json_direct_from_bytes<R: DeserializeOwned, F>(
        &self,
        request_bytes: &[u8],
        select_call: F
    ) -> Result<R, String>
        where F: FnOnce(&PluginVTableV1) -> PluginEndpointFnV1
    {
        self.with_instance(|context, vtable| {
            let mut response = AbiBuffer::EMPTY;
            let status = unsafe {
                select_call(vtable)(context, AbiSlice::from_bytes(request_bytes), &mut response)
            };
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
            let bytes = (unsafe { response.to_vec() }).ok_or_else(||
                "plugin returned an invalid response buffer".to_owned()
            )?;
            unsafe {
                (vtable.free_buffer)(response);
            }
            serde_json
                ::from_slice(&bytes)
                .map_err(|error| format!("failed to decode plugin response: {error}"))
        })?
    }

    pub(crate) fn name(self: &Arc<Self>) -> Result<String, String> {
        let bytes = self
            .call_with_timeout_internal("name", |runtime| {
                runtime.call_buffer(|vtable| vtable.name)
            })
            .map_err(|error| error.to_string())?;
        let name = serde_json
            ::from_slice::<String>(&bytes)
            .map_err(|error| format!("failed to decode plugin name: {error}"))?;
        self.set_name(name.clone());
        Ok(name)
    }

    pub(crate) fn manifest(self: &Arc<Self>) -> Result<PluginManifest, String> {
        let bytes = self
            .call_with_timeout_internal("manifest", |runtime| {
                runtime.call_buffer(|vtable| vtable.manifest)
            })
            .map_err(|error| error.to_string())?;
        serde_json
            ::from_slice(&bytes)
            .map_err(|error| format!("failed to decode plugin manifest: {error}"))
    }

    pub(crate) fn is_loaded(self: &Arc<Self>) -> Result<bool, String> {
        self.call_with_timeout("is_loaded", |runtime| {
            runtime.with_instance(|context, vtable| {
                let status = unsafe { (vtable.is_loaded)(context) };
                match status {
                    PLUGIN_STATUS_OK => Ok(true),
                    PLUGIN_STATUS_NOT_INITIALIZED => Ok(false),
                    _ => Err(format!("plugin ABI is_loaded call failed with status {status}")),
                }
            })?
        }).map_err(|error| error.to_string())
    }

    pub(crate) fn handle_endpoint(
        self: &Arc<Self>,
        request: PluginContracts::ApiContract::PluginEndpointRequest
    ) -> Result<
        PluginContracts::ApiContract::PluginEndpointResponse,
        PluginContracts::ApiContract::PluginEndpointError
    > {
        self
            .call_with_timeout("handle_endpoint", move |runtime| {
                let request_bytes = serde_json
                    ::to_vec(&request)
                    .map_err(|error| format!("failed to encode plugin request: {error}"))?;
                runtime.call_json_direct_from_bytes(&request_bytes, |vtable| vtable.handle_endpoint)
            })
            .map_err(|error| {
                match error {
                    PluginCallFailure::Timeout(timeout) =>
                        PluginContracts::ApiContract::PluginEndpointError::new(
                            504,
                            "plugin_timeout",
                            "The plugin did not respond within the configured timeout",
                            Some(format!("plugin call timeout: {:?}", timeout))
                        ),
                    PluginCallFailure::Busy =>
                        PluginContracts::ApiContract::PluginEndpointError::new(
                            503,
                            "plugin_overloaded",
                            "The plugin is currently handling its maximum number of concurrent calls",
                            None
                        ),
                    PluginCallFailure::Failed(message) =>
                        PluginContracts::ApiContract::PluginEndpointError::internal(
                            "plugin_abi_error",
                            "The plugin ABI call failed",
                            message
                        ),
                }
            })?
    }

    pub(crate) fn on_session_closed(
        self: &Arc<Self>,
        context: &PluginContracts::ApiContract::PluginSessionContext
    ) -> Result<(), PluginContracts::ApiContract::PluginLifecycleError> {
        let context = context.clone();
        self
            .call_with_timeout("on_session_closed", move |runtime| {
                let request_bytes = serde_json
                    ::to_vec(&context)
                    .map_err(|error|
                        format!("failed to encode session lifecycle request: {error}")
                    )?;
                runtime.call_json_direct_from_bytes(
                    &request_bytes,
                    |vtable| vtable.on_session_closed
                )
            })
            .map_err(|error| {
                match error {
                    PluginCallFailure::Timeout(_) =>
                        PluginContracts::ApiContract::PluginLifecycleError::new(
                            "plugin_timeout",
                            "Plugin session cleanup exceeded the configured timeout"
                        ),
                    PluginCallFailure::Busy =>
                        PluginContracts::ApiContract::PluginLifecycleError::new(
                            "plugin_overloaded",
                            "Plugin session cleanup was rejected because the plugin is overloaded"
                        ),
                    PluginCallFailure::Failed(message) =>
                        PluginContracts::ApiContract::PluginLifecycleError::new(
                            "plugin_abi_error",
                            message
                        ),
                }
            })?
    }

    pub(crate) fn execute_database(
        self: &Arc<Self>,
        request: DatabaseContracts::DatabaseRequest
    ) -> Result<DatabaseContracts::DatabaseResponse, DatabaseContracts::DatabaseError> {
        self
            .call_with_timeout("execute_database", move |runtime| {
                let request_bytes = serde_json
                    ::to_vec(&request)
                    .map_err(|error| format!("failed to encode database request: {error}"))?;
                runtime.call_json_direct_from_bytes(
                    &request_bytes,
                    |vtable| vtable.execute_database
                )
            })
            .map_err(|error| {
                match error {
                    PluginCallFailure::Timeout(timeout) =>
                        DatabaseContracts::DatabaseError::new(
                            DatabaseContracts::DatabaseErrorKind::Query,
                            format!("plugin database call timed out after {:?}", timeout)
                        ),
                    PluginCallFailure::Busy =>
                        DatabaseContracts::DatabaseError::new(
                            DatabaseContracts::DatabaseErrorKind::Query,
                            "plugin database concurrency limit reached"
                        ),
                    PluginCallFailure::Failed(message) =>
                        DatabaseContracts::DatabaseError::new(
                            DatabaseContracts::DatabaseErrorKind::Decode,
                            message
                        ),
                }
            })?
    }

    fn start_lifecycle_operation(
        &self,
        operation: PluginLifecycleOperation,
        callback: PluginLoadFnV1,
        context: *mut std::ffi::c_void
    ) -> Result<(), String> {
        let mut pending = self.pending_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending.is_some() {
            return Err("a plugin lifecycle callback is already in progress".to_owned());
        }

        let context = context as usize;
        let receiver = self.lifecycle_executor.submit(
            Box::new(move || {
                let context = context as *mut std::ffi::c_void;
                Ok(unsafe { callback(context) })
            })
        )?;
        *pending = Some(PendingPluginLifecycle { operation, receiver });
        Ok(())
    }

    fn start_destroy_operation(
        &self,
        callback: PluginDestroyFnV1,
        instance: *mut PluginInstanceV1
    ) -> Result<(), String> {
        let mut pending = self.pending_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending.is_some() {
            return Err("a plugin lifecycle callback is already in progress".to_owned());
        }

        let instance = instance as usize;
        let receiver = self.lifecycle_executor.submit(
            Box::new(move || {
                let instance = instance as *mut PluginInstanceV1;
                Ok(unsafe { callback(instance) })
            })
        )?;
        *pending = Some(PendingPluginLifecycle {
            operation: PluginLifecycleOperation::Destroy,
            receiver,
        });
        Ok(())
    }

    fn wait_for_pending_lifecycle(
        &self
    ) -> Result<Option<(PluginLifecycleOperation, Result<i32, String>)>, bool> {
        let pending = {
            let mut pending = self.pending_lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            pending.take()
        };
        let Some(pending) = pending else {
            return Ok(None);
        };

        match pending.receiver.recv_timeout(self.config.lifecycle_timeout) {
            Ok(result) => Ok(Some((pending.operation, result))),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let mut slot = self.pending_lifecycle
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *slot = Some(pending);
                Err(true)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Ok(
                    Some((
                        pending.operation,
                        Err("plugin lifecycle worker disconnected".to_owned()),
                    ))
                )
            }
        }
    }

    fn validate_lifecycle_instance(
        &self
    ) -> Result<(*mut PluginInstanceV1, PluginLoadFnV1, PluginDestroyFnV1), String> {
        let state = self.read_state();
        let instance = state.instance.ok_or_else(|| "plugin runtime is unloaded".to_owned())?;
        let instance_ref = unsafe { &*instance };
        if instance_ref.vtable.is_null() || instance_ref.context.is_null() {
            return Err("plugin runtime contains invalid ABI pointers".to_owned());
        }
        let vtable = unsafe { &*instance_ref.vtable };
        if !vtable.is_compatible() {
            return Err("plugin runtime contains an incompatible ABI vtable".to_owned());
        }
        Ok((instance, vtable.on_unload, vtable.destroy))
    }

    fn complete_destroy_success(&self) -> bool {
        let mut state = match self.state.write() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.instance.take().is_none() {
            return false;
        }
        self.set_health(PluginHealthState::Unloaded);
        self.unloading.store(false, Ordering::Release);
        self.worker_pool.shutdown();
        self.lifecycle_executor.shutdown();
        true
    }

    /// Quiesces the plugin with a bounded wait. A timeout never forcefully unloads a library while
    /// plugin work is still executing or queued; the manager keeps the plugin registered so the
    /// operation can be retried safely after all admitted calls drain.
    pub(crate) fn unload(&self) -> bool {
        // A prior lifecycle callback may still be finishing after its caller-side timeout. In that
        // case the runtime remains in `Stopping`; a subsequent unload retries by first observing
        // the actual callback result. No lifecycle stage is invoked concurrently.
        if
            self.pending_lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_some()
        {
            match self.wait_for_pending_lifecycle() {
                Err(true) => {
                    self.set_health_error(
                        PluginHealthState::Stopping,
                        format!(
                            "plugin lifecycle callback is still running after {:?}",
                            self.config.lifecycle_timeout
                        )
                    );
                    return false;
                }
                Err(false) => {
                    self.unloading.store(false, Ordering::Release);
                    self.set_health_error(
                        PluginHealthState::Failed,
                        "plugin lifecycle callback failed".to_string()
                    );
                    return false;
                }
                Ok(None) => {}
                Ok(Some((operation, result))) => {
                    match (operation, result) {
                        (PluginLifecycleOperation::OnUnload, Ok(status)) if
                            status == PLUGIN_STATUS_OK
                        => {}
                        (PluginLifecycleOperation::OnUnload, Ok(status)) => {
                            self.unloading.store(false, Ordering::Release);
                            self.set_health_error(
                                PluginHealthState::Failed,
                                format!("plugin on_unload returned status {status}")
                            );
                            return false;
                        }
                        (PluginLifecycleOperation::Destroy, Ok(status)) if
                            status == PLUGIN_STATUS_OK
                        => {
                            return self.complete_destroy_success();
                        }
                        (PluginLifecycleOperation::Destroy, Ok(status)) => {
                            self.unloading.store(false, Ordering::Release);
                            self.set_health_error(
                                PluginHealthState::Failed,
                                format!("plugin destroy returned status {status}")
                            );
                            return false;
                        }
                        (operation, Err(message)) => {
                            self.unloading.store(false, Ordering::Release);
                            self.set_health_error(
                                PluginHealthState::Failed,
                                format!(
                                    "plugin lifecycle operation {operation:?} failed: {message}"
                                )
                            );
                            return false;
                        }
                    }
                }
            }
        } else if self.unloading.swap(true, Ordering::AcqRel) {
            return false;
        }

        self.set_health(PluginHealthState::Stopping);

        let deadline = Instant::now() + self.config.unload_timeout;
        if !self.gate.wait_until_idle(deadline) {
            self.unloading.store(false, Ordering::Release);
            let message = format!(
                "plugin unload timed out after {:?} while waiting for in-flight calls to drain",
                self.config.unload_timeout
            );
            self.set_health_error(PluginHealthState::Degraded, message);
            return false;
        }

        let (instance, on_unload, destroy) = match self.validate_lifecycle_instance() {
            Ok(value) => value,
            Err(message) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(PluginHealthState::Failed, message);
                return false;
            }
        };

        if
            let Err(message) = self.start_lifecycle_operation(
                PluginLifecycleOperation::OnUnload,
                on_unload,
                unsafe {
                    (*instance).context
                }
            )
        {
            self.unloading.store(false, Ordering::Release);
            self.set_health_error(PluginHealthState::Failed, message);
            return false;
        }

        match self.wait_for_pending_lifecycle() {
            Err(true) => {
                let message = format!(
                    "plugin on_unload timed out after {:?}; lifecycle callback remains active and the runtime stays registered",
                    self.config.lifecycle_timeout
                );
                self.set_health_error(PluginHealthState::Stopping, message);
                false
            }
            Ok(Some((PluginLifecycleOperation::OnUnload, Ok(status)))) if
                status == PLUGIN_STATUS_OK
            => {
                if let Err(message) = self.start_destroy_operation(destroy, instance) {
                    self.unloading.store(false, Ordering::Release);
                    self.set_health_error(PluginHealthState::Failed, message);
                    return false;
                }

                match self.wait_for_pending_lifecycle() {
                    Err(true) => {
                        let message = format!(
                            "plugin destroy timed out after {:?}; lifecycle callback remains active and the runtime stays registered",
                            self.config.lifecycle_timeout
                        );
                        self.set_health_error(PluginHealthState::Stopping, message);
                        false
                    }
                    Ok(Some((PluginLifecycleOperation::Destroy, Ok(status)))) if
                        status == PLUGIN_STATUS_OK
                    => self.complete_destroy_success(),
                    Ok(Some((PluginLifecycleOperation::Destroy, Ok(status)))) => {
                        self.unloading.store(false, Ordering::Release);
                        self.set_health_error(
                            PluginHealthState::Failed,
                            format!("plugin destroy returned status {status}")
                        );
                        false
                    }
                    Ok(Some((_, Err(message)))) => {
                        self.unloading.store(false, Ordering::Release);
                        self.set_health_error(
                            PluginHealthState::Failed,
                            format!("plugin destroy failed: {message}")
                        );
                        false
                    }
                    Ok(None) => {
                        self.unloading.store(false, Ordering::Release);
                        self.set_health_error(
                            PluginHealthState::Failed,
                            "plugin destroy lifecycle result was unexpectedly absent"
                        );
                        false
                    }
                    Ok(Some((PluginLifecycleOperation::OnUnload, Ok(status)))) => {
                        self.unloading.store(false, Ordering::Release);
                        self.set_health_error(
                            PluginHealthState::Failed,
                            format!(
                                "unexpected on_unload result while waiting for plugin destroy: {status}"
                            )
                        );
                        false
                    }
                    Err(false) => {
                        self.unloading.store(false, Ordering::Release);
                        self.set_health_error(
                            PluginHealthState::Failed,
                            "plugin destroy lifecycle wait failed"
                        );
                        false
                    }
                }
            }
            Ok(Some((PluginLifecycleOperation::OnUnload, Ok(status)))) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    format!("plugin on_unload returned status {status}")
                );
                false
            }
            Ok(Some((PluginLifecycleOperation::OnUnload, Err(message)))) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    format!("plugin on_unload failed: {message}")
                );
                false
            }
            Ok(Some((PluginLifecycleOperation::Destroy, Ok(status)))) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    format!("plugin destroy returned status {status} after on_unload completed")
                );
                false
            }
            Ok(Some((PluginLifecycleOperation::Destroy, Err(message)))) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    format!("plugin destroy failed after on_unload completed: {message}")
                );
                false
            }
            Ok(None) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "plugin on_unload lifecycle result was unexpectedly absent"
                );
                false
            }
            Err(false) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "plugin on_unload lifecycle wait failed"
                );
                false
            }
        }
    }
}

impl Drop for PluginRuntime {
    fn drop(&mut self) {
        self.unloading.store(true, Ordering::Release);
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
                let _ = (vtable.on_unload)(instance_ref.context);
                let _ = (vtable.destroy)(instance);
            })
        );
    }
}

/// Public trait adapter preserving the existing `IPlugin`-based Factory API while the actual
/// dynamic-library boundary is represented only by `PluginRuntime` and the stable ABI.
#[derive(Clone)]
pub struct PluginProxy {
    runtime: Arc<PluginRuntime>,
}

impl PluginProxy {
    pub(crate) fn new(runtime: Arc<PluginRuntime>) -> Self {
        Self { runtime }
    }
}

impl IPlugin for PluginProxy {
    fn execute_database(
        &self,
        request: DatabaseContracts::DatabaseRequest
    ) -> Result<DatabaseContracts::DatabaseResponse, DatabaseContracts::DatabaseError> {
        self.runtime.execute_database(request)
    }

    fn isLoaded(&self) -> bool {
        self.runtime.is_loaded().unwrap_or(false)
    }

    fn manifest(&self) -> PluginManifest {
        self.runtime.manifest().unwrap_or_else(|_| PluginManifest::without_api("UnavailablePlugin"))
    }

    fn handle_endpoint(
        &self,
        request: PluginContracts::ApiContract::PluginEndpointRequest
    ) -> Result<
        PluginContracts::ApiContract::PluginEndpointResponse,
        PluginContracts::ApiContract::PluginEndpointError
    > {
        self.runtime.handle_endpoint(request)
    }

    fn name(&self) -> String {
        self.runtime.name().unwrap_or_else(|_| "UnavailablePlugin".to_owned())
    }

    fn to_box(&self) -> Box<dyn IPlugin> {
        Box::new(self.clone())
    }

    fn on_load(&mut self) -> bool {
        self.isLoaded()
    }

    fn on_load_with_host(
        &mut self,
        _host_services: &PluginContracts::ApiContract::PluginHostServices
    ) -> bool {
        self.isLoaded()
    }

    fn on_unload(&mut self) -> bool {
        self.runtime.unload()
    }

    fn on_session_closed(
        &self,
        context: &PluginContracts::ApiContract::PluginSessionContext
    ) -> Result<(), PluginContracts::ApiContract::PluginLifecycleError> {
        self.runtime.on_session_closed(context)
    }
}

pub(crate) struct PluginItem {
    pub(crate) path: PathBuf,
    manifest: PluginManifest,
    runtime: Arc<PluginRuntime>,
}

impl PluginItem {
    pub(crate) fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    pub(crate) fn runtime(&self) -> Arc<PluginRuntime> {
        Arc::clone(&self.runtime)
    }

    pub(crate) fn health(&self) -> PluginHealthSnapshot {
        self.runtime.snapshot()
    }
}

#[derive(Default)]
pub(crate) struct PluginManager {
    pub(crate) paths: HashMap<PathBuf, i64>,
    pub(crate) plugins: HashMap<i64, PluginItem>,
    catalog: PluginCatalog,
}

impl PluginManager {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn plugin_count(&self) -> usize {
        self.plugins.len()
    }

    pub(crate) fn get_runtime(&self, id: i64) -> Option<Arc<PluginRuntime>> {
        self.plugins.get(&id).map(PluginItem::runtime)
    }

    pub(crate) fn get_manifest(&self, id: i64) -> Option<&PluginManifest> {
        self.plugins.get(&id).map(PluginItem::manifest)
    }

    pub(crate) fn catalog(&self) -> PluginCatalog {
        self.catalog.clone()
    }

    pub(crate) fn plugin_ids(&self) -> Vec<i64> {
        let mut ids: Vec<i64> = self.plugins.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Discovers plugin library paths without loading or executing any plugin code.
    pub(crate) fn discover_plugin_paths(
        plugin_path: &str
    ) -> Result<Vec<Result<PathBuf, String>>, PluginManagerError> {
        let pattern = Self::build_plugin_pattern(plugin_path);
        let pattern_str = pattern
            .to_str()
            .ok_or_else(|| PluginManagerError::InvalidPluginPath(pattern.clone()))?;

        let entries = glob(pattern_str).map_err(|source| PluginManagerError::InvalidGlobPattern {
            pattern: pattern_str.to_owned(),
            source,
        })?;

        let mut paths = Vec::new();
        let mut errors = Vec::new();

        for entry in entries {
            match entry {
                Ok(path) => paths.push(Self::normalize_path(path.as_path())),
                Err(error) => errors.push(Err(format_glob_error(&error))),
            }
        }

        paths.sort();
        paths.dedup();

        let mut result = paths.into_iter().map(Ok).collect::<Vec<_>>();
        result.extend(errors);
        Ok(result)
    }

    /// Builds a candidate catalog without mutating manager state.
    pub(crate) fn catalog_with_manifest(
        &self,
        manifest: &PluginManifest
    ) -> Result<PluginCatalog, PluginManagerError> {
        let mut manifests = self
            .plugin_ids()
            .into_iter()
            .filter_map(|id| self.get_manifest(id).cloned())
            .collect::<Vec<_>>();
        manifests.push(manifest.clone());
        PluginCatalog::from_manifests(manifests).map_err(PluginManagerError::InvalidCatalog)
    }

    /// Registers a fully initialized plugin. No plugin callback is invoked here.
    pub(crate) fn insert_loaded_plugin(
        &mut self,
        canonical_path: &Path,
        loaded: LoadedPlugin,
        catalog: PluginCatalog
    ) -> i64 {
        let id = self.next_plugin_id();
        let item = PluginItem {
            path: canonical_path.to_path_buf(),
            manifest: loaded.manifest.clone(),
            runtime: loaded.runtime,
        };

        self.plugins.insert(id, item);
        self.paths.insert(canonical_path.to_path_buf(), id);
        self.catalog = catalog;
        id
    }

    /// Detaches a plugin from the registry without invoking plugin lifecycle code.
    pub(crate) fn detach_plugin(&mut self, id: i64) -> Result<PluginItem, PluginManagerError> {
        let manifests = self
            .plugin_ids()
            .into_iter()
            .filter(|plugin_id| *plugin_id != id)
            .filter_map(|plugin_id| self.get_manifest(plugin_id).cloned())
            .collect::<Vec<_>>();
        let new_catalog = PluginCatalog::from_manifests(manifests).map_err(
            PluginManagerError::InvalidCatalog
        )?;

        let item = self.plugins.remove(&id).ok_or(PluginManagerError::PluginNotFound { id })?;
        self.paths.remove(&item.path);
        self.catalog = new_catalog;
        Ok(item)
    }

    pub(crate) fn load_plugin(
        path: &Path,
        config: PluginRuntimeConfig
    ) -> Result<LoadedPlugin, PluginManagerError> {
        let library = (unsafe { Library::new(path) }).map_err(|source| {
            PluginManagerError::LibraryLoad {
                path: path.to_path_buf(),
                source,
            }
        })?;

        let constructor: Symbol<PluginCreateFnV1> = (
            unsafe {
                library.get(PLUGIN_CONSTRUCTOR_SYMBOL_V1)
            }
        ).map_err(|source| PluginManagerError::ConstructorLoad {
            path: path.to_path_buf(),
            symbol: "InstantiateV1",
            source,
        })?;

        let plugin_directory = path.parent().unwrap_or_else(|| Path::new("."));
        let plugin_directory_bytes = plugin_directory.to_string_lossy().into_owned().into_bytes();
        let plugin_directory_slice = AbiSlice::from_bytes(&plugin_directory_bytes);
        let database = super::Factory::database_capability();
        let host_services = PluginHostServicesV1::new(
            host_log,
            database.map(|capability| capability.execute),
            database.map(|capability| capability.free),
            plugin_directory_slice
        );

        // The constructor performs plugin-local initialization before it returns. The borrowed
        // plugin directory therefore remains valid for every initialization callback without
        // requiring the plugin to retain host-owned memory.
        let instance = unsafe { constructor(&host_services) };
        let runtime = PluginRuntime::new(instance, library, config).map_err(|message| {
            PluginManagerError::PluginAbiIncompatible {
                path: path.to_path_buf(),
                message,
            }
        })?;

        // Metadata is untrusted plugin code too. Both calls execute through the same bounded
        // worker pool, timeout, panic-isolation, and lifecycle/accounting path as endpoint calls.
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

    fn next_plugin_id(&self) -> i64 {
        self.plugins
            .keys()
            .copied()
            .max()
            .map_or(PLUGIN_AREA_ID, |max_id| max_id + 1)
    }

    fn build_plugin_pattern(plugin_path: &str) -> PathBuf {
        Path::new(plugin_path).join(Self::library_glob())
    }

    fn library_glob() -> &'static str {
        #[cfg(target_os = "linux")]
        {
            "*.so"
        }
        #[cfg(target_os = "macos")]
        {
            "*.dylib"
        }
        #[cfg(target_os = "windows")]
        {
            "*.dll"
        }
    }

    pub(crate) fn normalize_path(path: &Path) -> PathBuf {
        path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
    }
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
        _ => { LevelFilter::Error }
    };

    Common::Logging::Logger::Log::Dump(
        "{}",
        Common::Logging::LogConstants::LOGGERDEF,
        level_filter,
        &[message.as_str()]
    );
}

pub(crate) struct LoadedPlugin {
    runtime: Arc<PluginRuntime>,
    manifest: PluginManifest,
}

impl LoadedPlugin {
    pub(crate) fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }
}

fn format_glob_error(error: &GlobError) -> String {
    match error.path().to_str() {
        Some(path) => format!("{} ({})", path, error.error()),
        None => error.error().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::PluginCallTracker;

    #[test]
    fn timed_out_call_lifecycle_is_closed_by_the_worker() {
        let tracker = PluginCallTracker::new();
        let mut timeout_bookkeeping = false;

        assert!(tracker.mark_timed_out(|| timeout_bookkeeping = true));
        assert!(timeout_bookkeeping);
        assert!(tracker.complete());
    }

    #[test]
    fn completed_call_wins_the_timeout_race() {
        let tracker = PluginCallTracker::new();

        assert!(!tracker.complete());
        assert!(!tracker.mark_timed_out(|| panic!("completed call must not be timed out")));
    }
}
