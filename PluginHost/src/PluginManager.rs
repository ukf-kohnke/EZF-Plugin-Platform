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
    PluginEndpointFnV1,
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
        atomic::{ AtomicBool, AtomicU64, AtomicU8, Ordering },
    },
    thread,
    time::{ Duration, Instant },
};

const PLUGIN_AREA_ID: i64 = 0x00ff_ffff;
const DEFAULT_PLUGIN_CALL_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_PLUGIN_UNLOAD_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_PLUGIN_MAX_CONCURRENT_CALLS: usize = 64;
const MIN_PLUGIN_TIMEOUT_MS: u64 = 1;
const MAX_PLUGIN_TIMEOUT_MS: u64 = 10 * 60 * 1_000;
const MAX_PLUGIN_CONCURRENT_CALLS: usize = 1_024;

/// Runtime behavior limits for one plugin.
///
/// The defaults are intentionally conservative enough for normal API/database work while still
/// bounding the amount of work a misbehaving plugin can consume. Production deployments may
/// override them globally through environment variables:
/// `EZF_PLUGIN_CALL_TIMEOUT_MS`, `EZF_PLUGIN_UNLOAD_TIMEOUT_MS`, and
/// `EZF_PLUGIN_MAX_CONCURRENT_CALLS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PluginRuntimeConfig {
    pub call_timeout: Duration,
    pub unload_timeout: Duration,
    pub max_concurrent_calls: usize,
}

impl Default for PluginRuntimeConfig {
    fn default() -> Self {
        Self {
            call_timeout: Duration::from_millis(DEFAULT_PLUGIN_CALL_TIMEOUT_MS),
            unload_timeout: Duration::from_millis(DEFAULT_PLUGIN_UNLOAD_TIMEOUT_MS),
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

impl PluginHealthState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Loading,
            1 => Self::Ready,
            2 => Self::Degraded,
            3 => Self::Stopping,
            4 => Self::Failed,
            5 => Self::Unloaded,
            _ => Self::Failed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginHealthSnapshot {
    pub name: String,
    pub state: PluginHealthState,
    pub active_calls: usize,
    pub total_calls: u64,
    pub failed_calls: u64,
    pub timed_out_calls: u64,
    pub rejected_calls: u64,
    pub last_error: Option<String>,
}

struct PluginCallGate {
    state: Mutex<usize>,
    max_concurrent_calls: usize,
}

impl PluginCallGate {
    fn new(max_concurrent_calls: usize) -> Self {
        Self {
            state: Mutex::new(0),
            max_concurrent_calls: max_concurrent_calls.max(1),
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
}

struct PluginCallPermit {
    gate: Arc<PluginCallGate>,
}

impl Drop for PluginCallPermit {
    fn drop(&mut self) {
        let mut active = self.gate.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        *active = (*active).saturating_sub(1);
    }
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
/// The dynamic library and opaque plugin instance remain together. Request calls hold a shared
/// read lock, so independent calls can execute concurrently. Unload takes the exclusive write
/// lock and therefore waits until every in-flight plugin call has returned before invoking
/// `on_unload`, destroying the instance, and allowing the runtime to be dropped.
pub(crate) struct PluginRuntime {
    state: RwLock<PluginRuntimeState>,
    unloading: AtomicBool,
    config: PluginRuntimeConfig,
    gate: Arc<PluginCallGate>,
    health: AtomicU8,
    total_calls: AtomicU64,
    failed_calls: AtomicU64,
    timed_out_calls: AtomicU64,
    rejected_calls: AtomicU64,
    last_error: Mutex<Option<String>>,
    name: Mutex<Option<String>>,
}

struct PluginRuntimeState {
    instance: Option<*mut PluginInstanceV1>,
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

        Ok(
            Arc::new(Self {
                state: RwLock::new(PluginRuntimeState {
                    instance: Some(instance),
                    library,
                }),
                unloading: AtomicBool::new(false),
                gate: Arc::new(PluginCallGate::new(config.max_concurrent_calls)),
                config,
                health: AtomicU8::new(PluginHealthState::Ready as u8),
                total_calls: AtomicU64::new(0),
                failed_calls: AtomicU64::new(0),
                timed_out_calls: AtomicU64::new(0),
                rejected_calls: AtomicU64::new(0),
                last_error: Mutex::new(None),
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

        match PluginHealthState::from_u8(self.health.load(Ordering::Acquire)) {
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
        self.health.store(state as u8, Ordering::Release);
    }

    fn record_success(&self) {
        self.set_health(PluginHealthState::Ready);
        let mut error = self.last_error.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        *error = None;
    }

    fn record_failure(&self, message: impl Into<String>, timed_out: bool) {
        let message = message.into();
        self.failed_calls.fetch_add(1, Ordering::Relaxed);
        if timed_out {
            self.timed_out_calls.fetch_add(1, Ordering::Relaxed);
        }
        self.set_health(PluginHealthState::Degraded);
        let mut error = self.last_error.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        *error = Some(message);
    }

    fn snapshot(&self) -> PluginHealthSnapshot {
        let state = PluginHealthState::from_u8(self.health.load(Ordering::Acquire));
        let name = self.name
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .unwrap_or_else(|| "<initializing>".to_owned());
        PluginHealthSnapshot {
            name,
            state,
            active_calls: self.gate.active_calls(),
            total_calls: self.total_calls.load(Ordering::Relaxed),
            failed_calls: self.failed_calls.load(Ordering::Relaxed),
            timed_out_calls: self.timed_out_calls.load(Ordering::Relaxed),
            rejected_calls: self.rejected_calls.load(Ordering::Relaxed),
            last_error: self.last_error
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
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

        self.total_calls.fetch_add(1, Ordering::Relaxed);
        let runtime = Arc::clone(self);
        let (sender, receiver) = mpsc::sync_channel(1);
        let thread_name = format!("ezf-plugin-{operation}");

        if
            thread::Builder
                ::new()
                .name(thread_name)
                .spawn(move || {
                    let result = std::panic::catch_unwind(
                        std::panic::AssertUnwindSafe(|| call(&runtime))
                    );
                    let result = match result {
                        Ok(result) => result,
                        Err(_) => Err(format!("plugin operation '{operation}' panicked")),
                    };
                    // Release admission before publishing completion. Otherwise the caller can
                    // observe the completed result and immediately submit a new call while the
                    // previous permit is still held, producing a spurious `Busy` rejection.
                    drop(permit);
                    let _ = sender.send(result);
                })
                .is_err()
        {
            self.record_failure("failed to spawn plugin call worker thread", false);
            return Err(
                PluginCallFailure::Failed("failed to spawn plugin call worker thread".to_owned())
            );
        }

        match receiver.recv_timeout(self.config.call_timeout) {
            Ok(Ok(result)) => {
                self.record_success();
                Ok(result)
            }
            Ok(Err(error)) => {
                self.record_failure(error.clone(), false);
                Err(PluginCallFailure::Failed(error))
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let message = format!(
                    "plugin operation '{operation}' timed out after {:?}",
                    self.config.call_timeout
                );
                self.record_failure(message.clone(), true);
                Common::Logging::Logger::Log::Dump(
                    "{}",
                    Common::Logging::LogConstants::LOGGERDEF,
                    LevelFilter::Warn,
                    &[message.as_str()]
                );
                Err(PluginCallFailure::Timeout(self.config.call_timeout))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let message = format!("plugin operation '{operation}' worker disconnected");
                self.record_failure(message.clone(), false);
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

    pub(crate) fn name(&self) -> Result<String, String> {
        let bytes = self.call_buffer(|vtable| vtable.name)?;
        let name = serde_json
            ::from_slice::<String>(&bytes)
            .map_err(|error| format!("failed to decode plugin name: {error}"))?;
        self.set_name(name.clone());
        Ok(name)
    }

    pub(crate) fn manifest(&self) -> Result<PluginManifest, String> {
        let bytes = self.call_buffer(|vtable| vtable.manifest)?;
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

    /// Quiesces the plugin with a bounded wait. A timeout never forcefully unloads a library while
    /// plugin code is still executing; the manager keeps the plugin registered so the operation
    /// can be retried safely after the in-flight calls drain.
    pub(crate) fn unload(&self) -> bool {
        if self.unloading.swap(true, Ordering::AcqRel) {
            return false;
        }
        self.set_health(PluginHealthState::Stopping);

        let deadline = Instant::now() + self.config.unload_timeout;
        let mut state = loop {
            match self.state.try_write() {
                Ok(state) => {
                    break state;
                }
                Err(_) => {
                    if Instant::now() >= deadline {
                        self.unloading.store(false, Ordering::Release);
                        self.set_health(PluginHealthState::Degraded);
                        let message = format!(
                            "plugin unload timed out after {:?}",
                            self.config.unload_timeout
                        );
                        let mut error = self.last_error
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        *error = Some(message);
                        return false;
                    }
                    thread::sleep(Duration::from_millis(5));
                }
            }
        };

        let Some(instance) = state.instance else {
            self.set_health(PluginHealthState::Unloaded);
            return true;
        };

        let instance_ref = unsafe { &*instance };
        if instance_ref.vtable.is_null() || instance_ref.context.is_null() {
            self.unloading.store(false, Ordering::Release);
            self.set_health(PluginHealthState::Failed);
            return false;
        }
        let vtable = unsafe { &*instance_ref.vtable };
        if !vtable.is_compatible() {
            self.unloading.store(false, Ordering::Release);
            self.set_health(PluginHealthState::Failed);
            return false;
        }

        let status = unsafe { (vtable.on_unload)(instance_ref.context) };
        if status != PLUGIN_STATUS_OK {
            self.unloading.store(false, Ordering::Release);
            self.set_health(PluginHealthState::Failed);
            let mut error = self.last_error.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            *error = Some(format!("plugin on_unload returned status {status}"));
            return false;
        }

        let destroy_status = unsafe { (vtable.destroy)(instance) };
        if destroy_status != PLUGIN_STATUS_OK {
            self.unloading.store(false, Ordering::Release);
            self.set_health(PluginHealthState::Failed);
            let mut error = self.last_error.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            *error = Some(format!("plugin destroy returned status {destroy_status}"));
            Common::Logging::Logger::Log::Dump(
                "{}",
                Common::Logging::LogConstants::LOGGERDEF,
                LevelFilter::Error,
                &[
                    "Plugin instance destruction reported an ABI error; runtime stays loaded for retry.",
                ]
            );
            return false;
        }

        state.instance = None;
        self.set_health(PluginHealthState::Unloaded);
        true
    }
}

impl Drop for PluginRuntime {
    fn drop(&mut self) {
        self.unloading.store(true, Ordering::Release);
        let mut state = self.state.write().unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(instance) = state.instance.take() else {
            return;
        };

        let instance_ref = unsafe { &*instance };
        if instance_ref.vtable.is_null() || instance_ref.context.is_null() {
            return;
        }
        let vtable = unsafe { &*instance_ref.vtable };
        let _ = unsafe { (vtable.on_unload)(instance_ref.context) };
        let _ = unsafe { (vtable.destroy)(instance) };
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
