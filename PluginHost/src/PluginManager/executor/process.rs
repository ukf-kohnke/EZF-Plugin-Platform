//! Process-backed adapter for `PluginExecutor`.
//!
//! This first supervisor implementation deliberately runs one request at a time per worker because
//! the Phase-4 worker has a single-flight request loop. It owns the child handle and is the only
//! component that reaps it. Calls that exceed their deadline request cooperative cancellation over
//! IPC and force-terminate/reap the worker after a bounded grace period. There is no retry policy.

use super::{ PluginCallContext, PluginExecutor, PluginLifecycleStatus };
use super::super::{ LoadedPlugin, PluginRuntime, PluginRuntimeConfig };
use crate::Factory::PluginManagerError;
use PluginAbi::PLUGIN_ABI_VERSION_V2;
use PluginContracts::ApiContract::{
    PluginEndpointError,
    PluginEndpointResponse,
    PluginLifecycleError,
    PluginManifest,
};
use PluginIpc::{
    CancelControl,
    FrameLimits,
    HelloControl,
    InitializeControl,
    InitializedControl,
    InvokeControl,
    InvokeOperation,
    IpcErrorCode,
    IpcFrame,
    LoadPluginControl,
    MessageType,
    ProtocolErrorControl,
    ResultControl,
    RuntimeLimits,
    SandboxProfile,
    SandboxReadyControl,
    ShutdownCompleteControl,
    ShutdownControl,
    DEFAULT_MAX_PAYLOAD_BYTES,
    IPC_MAJOR_VERSION,
    IPC_MINOR_VERSION,
};
use std::path::{ Path, PathBuf };
use std::sync::atomic::{ AtomicBool, AtomicU64, Ordering };
use std::sync::{ mpsc, Mutex };
use std::thread::{ self, JoinHandle };
use std::time::{ Duration, Instant };
use std::{ fs, io };
use std::process::{ Child, Command, Stdio };
use std::sync::Arc;
use std::os::unix::fs::{ DirBuilderExt, PermissionsExt };
use std::net::Shutdown;
use std::os::unix::net::{ UnixListener, UnixStream };

const SOCKET_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const RECEIVE_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DEFAULT_CANCEL_GRACE: Duration = Duration::from_millis(500);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const WORKER_BINARY_NAME: &str = "ezf-plugin-worker";

static SOCKET_DIRECTORY_COUNTER: AtomicU64 = AtomicU64::new(1);

struct WorkerSupervisor {
    child: Mutex<Option<Child>>,
    exit_success: Mutex<Option<bool>>,
    terminated: AtomicBool,
}

impl WorkerSupervisor {
    fn new(child: Child) -> Self {
        Self {
            child: Mutex::new(Some(child)),
            exit_success: Mutex::new(None),
            terminated: AtomicBool::new(false),
        }
    }

    fn ensure_alive(&self) -> Result<(), String> {
        match self.poll_exit()? {
            None => Ok(()),
            Some(true) => Err("plugin worker has exited".to_owned()),
            Some(false) => Err("plugin worker exited unsuccessfully".to_owned()),
        }
    }

    /// `Some(success)` means the child has exited and its status has been collected/reaped.
    fn poll_exit(&self) -> Result<Option<bool>, String> {
        let mut child_guard = self.child.lock().unwrap_or_else(|p| p.into_inner());
        let Some(child) = child_guard.as_mut() else {
            let result = *self.exit_success.lock().unwrap_or_else(|p| p.into_inner());
            return Ok(result.or(Some(false)));
        };

        match child.try_wait() {
            Ok(None) => Ok(None),
            Ok(Some(status)) => {
                *child_guard = None;
                *self.exit_success.lock().unwrap_or_else(|p| p.into_inner()) = Some(
                    status.success()
                );
                self.terminated.store(true, Ordering::Release);
                Ok(Some(status.success()))
            }
            Err(error) => Err(format!("could not inspect plugin worker process: {error}")),
        }
    }

    fn wait_for_exit(&self, timeout: Duration) -> Result<Option<bool>, String> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.poll_exit()? {
                return Ok(Some(status));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            thread::sleep(
                RECEIVE_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now()))
            );
        }
    }

    /// Kills (if needed) and waits for the worker. No other component owns or waits on `Child`.
    fn terminate_and_reap(&self) -> Option<bool> {
        let mut child_guard = self.child.lock().unwrap_or_else(|p| p.into_inner());
        let Some(mut child) = child_guard.take() else {
            self.terminated.store(true, Ordering::Release);
            return *self.exit_success.lock().unwrap_or_else(|p| p.into_inner());
        };

        let status = match child.try_wait() {
            Ok(Some(status)) => Some(status),
            Ok(None) | Err(_) => {
                let _ = child.kill();
                child.wait().ok()
            }
        };
        let success = status.map(|status| status.success());
        *self.exit_success.lock().unwrap_or_else(|p| p.into_inner()) = success;
        self.terminated.store(true, Ordering::Release);
        success
    }
}

impl Drop for WorkerSupervisor {
    fn drop(&mut self) {
        let _ = self.terminate_and_reap();
    }
}

struct SocketDirectory(PathBuf);

impl SocketDirectory {
    fn create() -> Result<Self, String> {
        for _ in 0..16 {
            let counter = SOCKET_DIRECTORY_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env
                ::temp_dir()
                .join(format!("ezf-pr-{}-{counter}", std::process::id()));
            let result = fs::DirBuilder::new().mode(0o700).create(&path);
            match result {
                Ok(()) => {
                    return Ok(Self(path));
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    continue;
                }
                Err(error) => {
                    return Err(
                        format!("could not create private worker socket directory: {error}")
                    );
                }
            }
        }
        Err("could not allocate a unique worker socket directory".to_owned())
    }

    fn socket_path(&self) -> PathBuf {
        self.0.join("worker.sock")
    }

    fn cleanup(&self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

impl Drop for SocketDirectory {
    fn drop(&mut self) {
        self.cleanup();
    }
}

enum ReaderEvent {
    Frame(IpcFrame),
    Disconnected(String),
}

struct InvocationResponse {
    control: ResultControl,
    payload: Vec<u8>,
}

struct ProcessExecutor {
    supervisor: Arc<WorkerSupervisor>,
    writer: Mutex<UnixStream>,
    events: Mutex<mpsc::Receiver<ReaderEvent>>,
    call_gate: Mutex<()>,
    reader_thread: Mutex<Option<JoinHandle<()>>>,
    socket_directory: SocketDirectory,
    next_request_id: AtomicU64,
    limits: FrameLimits,
    cancellation_grace: Duration,
    shutdown_timeout: Duration,
    destroyed: AtomicBool,
}

impl ProcessExecutor {
    fn start(plugin_path: &Path, _config: &PluginRuntimeConfig) -> Result<Self, String> {
        if !unconfined_mode_explicitly_allowed() {
            return Err(
                "process execution currently requires EZF_PLUGIN_ALLOW_UNCONFINED=1 because no OS sandbox is implemented".to_owned()
            );
        }

        let plugin_path = plugin_path
            .canonicalize()
            .map_err(|error| {
                format!("could not canonicalize plugin path {}: {error}", plugin_path.display())
            })?;
        if !plugin_path.is_absolute() || !plugin_path.is_file() {
            return Err(
                format!("plugin path must be an absolute file path: {}", plugin_path.display())
            );
        }
        let plugin_directory = plugin_path.parent().unwrap_or_else(|| Path::new("/"));
        let worker_binary = resolve_worker_binary()?;
        let socket_directory = SocketDirectory::create()?;
        let socket_path = socket_directory.socket_path();
        let listener = UnixListener::bind(&socket_path).map_err(|error|
            format!("could not bind private worker IPC socket: {error}")
        )?;
        fs
            ::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("could not restrict worker IPC socket permissions: {error}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| format!("could not configure worker IPC listener: {error}"))?;

        // This is deliberately an explicit development-only launch. It must not be treated as a
        // production sandbox and will fail closed if the explicit opt-in is missing.
        let child = Command::new(&worker_binary)
            .arg("--ipc-socket")
            .arg(&socket_path)
            .arg("--allow-unconfined")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|error|
                format!("could not start worker {}: {error}", worker_binary.display())
            )?;
        let supervisor = Arc::new(WorkerSupervisor::new(child));
        let mut stream = accept_worker(&listener, &supervisor)?;
        drop(listener);
        stream
            .set_read_timeout(Some(HANDSHAKE_TIMEOUT))
            .map_err(|error| format!("could not set worker handshake read timeout: {error}"))?;
        stream
            .set_write_timeout(Some(HANDSHAKE_TIMEOUT))
            .map_err(|error| format!("could not set worker handshake write timeout: {error}"))?;

        let limits = FrameLimits::default();
        startup_round_trip(
            &mut stream,
            MessageType::Hello,
            1,
            &(HelloControl {
                protocol_major: IPC_MAJOR_VERSION,
                protocol_minor: IPC_MINOR_VERSION,
                worker_build: format!("PluginHost/{}", env!("CARGO_PKG_VERSION")),
            }),
            MessageType::Hello,
            limits
        )?;

        let cancellation_grace = configured_duration(
            "EZF_PLUGIN_PROCESS_CANCEL_GRACE_MS",
            DEFAULT_CANCEL_GRACE,
            Duration::from_millis(10),
            Duration::from_secs(30)
        );
        let shutdown_timeout = configured_duration(
            "EZF_PLUGIN_PROCESS_SHUTDOWN_TIMEOUT_MS",
            DEFAULT_SHUTDOWN_TIMEOUT,
            Duration::from_millis(100),
            Duration::from_secs(15)
        );
        startup_round_trip(
            &mut stream,
            MessageType::Initialize,
            2,
            &(InitializeControl {
                protocol_major: IPC_MAJOR_VERSION,
                protocol_minor: IPC_MINOR_VERSION,
                worker_build: format!("PluginHost/{}", env!("CARGO_PKG_VERSION")),
                plugin_path: plugin_path.to_string_lossy().into_owned(),
                plugin_directory: plugin_directory.to_string_lossy().into_owned(),
                // The capability broker is a later architecture phase. Do not advertise a
                // capability that this worker cannot service over IPC yet.
                allowed_capabilities: Vec::new(),
                runtime_limits: RuntimeLimits {
                    // The current worker request loop is single-flight. Host admission may still
                    // be concurrent, but this executor serializes requests to preserve framing.
                    max_concurrent_calls: 1,
                    max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES as u32,
                    cancellation_grace_ms: cancellation_grace
                        .as_millis()
                        .min(u64::MAX as u128) as u64,
                },
                sandbox_profile: SandboxProfile::DevelopmentUnconfined,
            }),
            MessageType::SandboxReady,
            limits
        )?;
        // `startup_round_trip` has checked SandboxReady's request ID, profile, and explicit
        // `sandbox_enforced = false` declaration. No plugin request is dispatched until the
        // subsequent Initialized response has also been validated.
        let load = IpcFrame::with_control(
            MessageType::LoadPlugin,
            3,
            &(LoadPluginControl { expected_abi_version: PLUGIN_ABI_VERSION_V2 })
        ).map_err(|error| error.to_string())?;
        load.write_to(&mut stream, limits).map_err(|error| error.to_string())?;
        let initialized_frame = IpcFrame::read_from(&mut stream, limits).map_err(|error|
            format!("worker failed while loading plugin: {error}")
        )?;
        ensure_response_type(&initialized_frame, MessageType::Initialized, 3)?;
        let initialized: InitializedControl = initialized_frame
            .decode_control()
            .map_err(|error| format!("invalid worker Initialized response: {error}"))?;
        if initialized.abi_version != PLUGIN_ABI_VERSION_V2 {
            return Err(
                format!("worker loaded unexpected plugin ABI version {}", initialized.abi_version)
            );
        }
        if initialized.plugin_name.trim().is_empty() {
            return Err("worker returned an empty plugin name".to_owned());
        }
        let manifest: serde_json::Value = serde_json
            ::from_str(&initialized.manifest_json)
            .map_err(|error| format!("worker returned invalid manifest JSON: {error}"))?;
        let manifest_name = manifest.get("name").and_then(serde_json::Value::as_str);
        if !manifest_name.is_some_and(|name| name.eq_ignore_ascii_case(&initialized.plugin_name)) {
            return Err("worker plugin name and manifest name do not match".to_owned());
        }

        // Disable startup timeouts before the long-lived reader takes ownership of a cloned stream.
        stream
            .set_read_timeout(None)
            .map_err(|error| format!("could not clear worker read timeout: {error}"))?;
        stream
            .set_write_timeout(None)
            .map_err(|error| format!("could not clear worker write timeout: {error}"))?;
        let reader = stream
            .try_clone()
            .map_err(|error| format!("could not clone worker IPC socket: {error}"))?;
        let (event_sender, event_receiver) = mpsc::channel();
        let reader_supervisor = Arc::clone(&supervisor);
        let reader_thread = thread::Builder
            ::new()
            .name(format!("ezf-plugin-ipc-reader-{}", std::process::id()))
            .spawn(move || reader_loop(reader, limits, event_sender, reader_supervisor))
            .map_err(|error| format!("could not start worker IPC reader: {error}"))?;

        log::warn!("starting plugin worker in DevelopmentUnconfined mode; no OS sandbox is active");
        Ok(Self {
            supervisor,
            writer: Mutex::new(stream),
            events: Mutex::new(event_receiver),
            call_gate: Mutex::new(()),
            reader_thread: Mutex::new(Some(reader_thread)),
            socket_directory,
            next_request_id: AtomicU64::new(100),
            limits,
            cancellation_grace,
            shutdown_timeout,
            destroyed: AtomicBool::new(false),
        })
    }

    fn invoke(
        &self,
        operation: InvokeOperation,
        payload: &[u8],
        context: &PluginCallContext
    ) -> Result<InvocationResponse, String> {
        let _call_guard = self.call_gate.lock().unwrap_or_else(|p| p.into_inner());
        self.supervisor.ensure_alive()?;
        if self.destroyed.load(Ordering::Acquire) {
            return Err("process plugin instance has already been destroyed".to_owned());
        }
        if context.is_deadline_expired() || context.cancellation_token().is_cancelled() {
            return Err(
                "plugin operation was cancelled or timed out before worker dispatch".to_owned()
            );
        }

        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let remaining = context.remaining_budget();
        let remaining_budget_ms = remaining
            .as_millis()
            .max(1)
            .min(u64::MAX as u128) as u64;
        let mut frame = IpcFrame::with_control(
            MessageType::Invoke,
            request_id,
            &(InvokeControl {
                operation,
                operation_id: context.operation_id(),
                remaining_budget_ms,
            })
        ).map_err(|error| format!("could not encode worker invoke request: {error}"))?;
        frame.payload = payload.to_vec();
        self.write_frame(&frame)?;

        let cancellation = context.cancellation_token();
        let mut cancellation_started: Option<Instant> = None;
        loop {
            if
                cancellation_started.is_none() &&
                (context.is_deadline_expired() || cancellation.is_cancelled())
            {
                let reason = if context.is_deadline_expired() {
                    "operation deadline expired"
                } else {
                    "operation cancellation requested"
                };
                let cancel = IpcFrame::with_control(
                    MessageType::Cancel,
                    request_id,
                    &(CancelControl { reason: Some(reason.to_owned()) })
                ).map_err(|error| format!("could not encode worker cancellation: {error}"))?;
                if let Err(error) = self.write_frame(&cancel) {
                    self.force_stop("failed to send cancellation to worker");
                    return Err(format!("could not send worker cancellation: {error}"));
                }
                cancellation_started = Some(Instant::now());
            }

            if let Some(started) = cancellation_started {
                if started.elapsed() >= self.cancellation_grace {
                    self.force_stop(
                        "worker did not complete after cancellation; process was terminated"
                    );
                    return Err(
                        format!(
                            "plugin operation {:?} exceeded its deadline/cancellation grace period; worker was terminated and reaped",
                            operation
                        )
                    );
                }
            }

            let wait_for = cancellation_started
                .map(|started|
                    self.cancellation_grace
                        .saturating_sub(started.elapsed())
                        .min(RECEIVE_POLL_INTERVAL)
                )
                .unwrap_or(RECEIVE_POLL_INTERVAL);
            let event = {
                let receiver = self.events.lock().unwrap_or_else(|p| p.into_inner());
                receiver.recv_timeout(wait_for)
            };
            match event {
                Ok(ReaderEvent::Frame(response)) => {
                    if response.message_type == MessageType::Log {
                        if let Ok(log) = response.decode_control::<PluginIpc::LogControl>() {
                            log_worker_message(log.level, &log.message);
                        }
                        continue;
                    }
                    if response.request_id != request_id {
                        self.force_stop("worker returned a response for an unknown request id");
                        return Err(
                            format!(
                                "worker protocol violation: expected response request_id {request_id}, received {}",
                                response.request_id
                            )
                        );
                    }
                    match response.message_type {
                        MessageType::Result => {
                            let control: ResultControl = match response.decode_control() {
                                Ok(control) => control,
                                Err(error) => {
                                    self.force_stop(
                                        "worker returned malformed Result control data"
                                    );
                                    return Err(format!("invalid worker Result control: {error}"));
                                }
                            };
                            // Deadline/cancellation wins if it was observed before consuming the
                            // result. A late result is not returned to the caller.
                            if
                                cancellation_started.is_none() &&
                                (context.is_deadline_expired() || cancellation.is_cancelled())
                            {
                                let cancel = IpcFrame::with_control(
                                    MessageType::Cancel,
                                    request_id,
                                    &(CancelControl {
                                        reason: Some(
                                            "operation completed after cancellation/deadline".to_owned()
                                        ),
                                    })
                                ).map_err(|error| error.to_string())?;
                                if self.write_frame(&cancel).is_err() {
                                    self.force_stop(
                                        "could not send cancellation after a late response"
                                    );
                                }
                                return Err(
                                    "plugin operation completed after its deadline/cancellation was observed".to_owned()
                                );
                            }
                            if cancellation_started.is_some() {
                                // The response arrived within the grace period. The worker remains
                                // healthy; its request loop consumes the queued Cancel before the
                                // next Invoke because writes are serialized by this executor.
                                return Err(
                                    "plugin operation was cancelled or timed out".to_owned()
                                );
                            }
                            return Ok(InvocationResponse { control, payload: response.payload });
                        }
                        MessageType::ProtocolError => {
                            let details: ProtocolErrorControl = response
                                .decode_control()
                                .unwrap_or(ProtocolErrorControl {
                                    code: IpcErrorCode::ProtocolViolation,
                                    message: "worker returned malformed ProtocolError".to_owned(),
                                });
                            self.force_stop("worker reported an IPC protocol error");
                            return Err(
                                format!(
                                    "worker protocol error {:?}: {}",
                                    details.code,
                                    details.message
                                )
                            );
                        }
                        other => {
                            self.force_stop("worker returned an unexpected message type");
                            return Err(
                                format!(
                                    "worker protocol violation: unexpected {:?} during Invoke",
                                    other
                                )
                            );
                        }
                    }
                }
                Ok(ReaderEvent::Disconnected(message)) => {
                    self.force_stop("worker IPC connection closed during Invoke");
                    return Err(
                        format!("plugin worker disconnected during {:?}: {message}", operation)
                    );
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(message) = self.supervisor.ensure_alive() {
                        self.force_stop("worker exited while an operation was active");
                        return Err(
                            format!("plugin worker exited during {:?}: {message}", operation)
                        );
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.force_stop("worker IPC reader disconnected");
                    return Err("plugin worker IPC reader disconnected".to_owned());
                }
            }
        }
    }

    fn invoke_payload(
        &self,
        operation: InvokeOperation,
        payload: &[u8],
        context: &PluginCallContext
    ) -> Result<Vec<u8>, String> {
        let response = self.invoke(operation, payload, context)?;
        if response.control.success {
            Ok(response.payload)
        } else {
            Err(
                response.control.error_message.unwrap_or_else(|| {
                    format!("worker reported {:?} for {:?}", response.control.error_code, operation)
                })
            )
        }
    }

    fn invoke_lifecycle(
        &self,
        operation: InvokeOperation,
        context: &PluginCallContext
    ) -> Result<PluginLifecycleStatus, String> {
        let response = self.invoke(operation, &[], context)?;
        let status = serde_json
            ::from_slice::<i32>(&response.payload)
            .map_err(|error| {
                response.control.error_message
                    .clone()
                    .unwrap_or_else(|| {
                        format!(
                            "worker returned invalid lifecycle status for {:?}: {error}",
                            operation
                        )
                    })
            })?;
        if status != PluginAbi::PLUGIN_STATUS_OK {
            return Ok(PluginLifecycleStatus::Failed(status));
        }
        if !response.control.success {
            return Err(
                response.control.error_message.unwrap_or_else(|| {
                    format!("worker reported {:?} failure without a non-zero ABI status", operation)
                })
            );
        }
        Ok(PluginLifecycleStatus::Success)
    }

    fn write_frame(&self, frame: &IpcFrame) -> Result<(), String> {
        let mut writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        frame.write_to(&mut *writer, self.limits).map_err(|error| error.to_string())
    }

    fn graceful_shutdown_after_destroy(&self) -> bool {
        let _call_guard = self.call_gate.lock().unwrap_or_else(|p| p.into_inner());
        if self.supervisor.ensure_alive().is_err() {
            self.force_stop("worker exited after destroy before graceful shutdown");
            return false;
        }

        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let frame = match
            IpcFrame::with_control(
                MessageType::Shutdown,
                request_id,
                &(ShutdownControl {
                    grace_budget_ms: self.shutdown_timeout.as_millis().min(u64::MAX as u128) as u64,
                })
            )
        {
            Ok(frame) => frame,
            Err(error) => {
                self.force_stop("could not encode graceful worker shutdown");
                log::warn!("could not encode graceful worker shutdown: {error}");
                return false;
            }
        };
        if let Err(error) = self.write_frame(&frame) {
            self.force_stop("could not send graceful worker shutdown");
            log::warn!("could not send graceful worker shutdown: {error}");
            return false;
        }

        let deadline = Instant::now() + self.shutdown_timeout;
        let mut graceful_response = None;
        while Instant::now() < deadline {
            let wait = RECEIVE_POLL_INTERVAL.min(
                deadline.saturating_duration_since(Instant::now())
            );
            let event = {
                let receiver = self.events.lock().unwrap_or_else(|p| p.into_inner());
                receiver.recv_timeout(wait)
            };
            match event {
                Ok(ReaderEvent::Frame(frame)) if
                    frame.request_id == request_id &&
                    frame.message_type == MessageType::ShutdownComplete
                => {
                    graceful_response = frame
                        .decode_control::<ShutdownCompleteControl>()
                        .ok()
                        .map(|v| v.graceful);
                    break;
                }
                Ok(ReaderEvent::Frame(frame)) if frame.message_type == MessageType::Log => {
                    if let Ok(log) = frame.decode_control::<PluginIpc::LogControl>() {
                        log_worker_message(log.level, &log.message);
                    }
                }
                Ok(ReaderEvent::Frame(frame)) => {
                    log::warn!(
                        "unexpected worker message {:?} during shutdown (request_id={})",
                        frame.message_type,
                        frame.request_id
                    );
                }
                Ok(ReaderEvent::Disconnected(_)) => {
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if self.supervisor.poll_exit().ok().flatten().is_some() {
                        break;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }

        let process_exit = self.supervisor.wait_for_exit(
            deadline.saturating_duration_since(Instant::now())
        );
        let clean_exit = matches!(process_exit, Ok(Some(true)));
        let graceful = graceful_response == Some(true) && clean_exit;
        if !graceful {
            log::warn!(
                "plugin worker required forced termination or did not report graceful shutdown after destroy; plugin code has been isolated by process exit"
            );
            let _ = self.supervisor.terminate_and_reap();
        }
        self.close_transport_and_join_reader();
        self.socket_directory.cleanup();
        graceful
    }

    fn force_stop(&self, reason: &str) {
        if !self.supervisor.terminated.load(Ordering::Acquire) {
            log::warn!("stopping plugin worker: {reason}");
        }
        let _ = self.supervisor.terminate_and_reap();
        self.close_transport_and_join_reader();
        self.socket_directory.cleanup();
    }

    fn close_transport_and_join_reader(&self) {
        let writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        let _ = writer.shutdown(Shutdown::Both);
        drop(writer);
        let handle = self.reader_thread
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(handle) = handle {
            if handle.thread().id() != thread::current().id() {
                let _ = handle.join();
            }
        }
    }
}

impl PluginExecutor for ProcessExecutor {
    fn validate(&self) -> Result<(), String> {
        self.supervisor.ensure_alive()?;
        if self.destroyed.load(Ordering::Acquire) {
            return Err("process plugin has already been destroyed".to_owned());
        }
        Ok(())
    }

    fn validate_for_lifecycle(&self) -> Result<(), String> {
        self.validate()
    }

    fn name_bytes(&self, context: &PluginCallContext) -> Result<Vec<u8>, String> {
        self.invoke_payload(InvokeOperation::Name, &[], context)
    }

    fn manifest_bytes(&self, context: &PluginCallContext) -> Result<Vec<u8>, String> {
        self.invoke_payload(InvokeOperation::Manifest, &[], context)
    }

    fn is_loaded(&self, context: &PluginCallContext) -> Result<bool, String> {
        let bytes = self.invoke_payload(InvokeOperation::IsLoaded, &[], context)?;
        serde_json
            ::from_slice(&bytes)
            .map_err(|error| format!("failed to decode worker is_loaded response: {error}"))
    }

    fn handle_endpoint(
        &self,
        request_bytes: &[u8],
        context: &PluginCallContext
    ) -> Result<Result<PluginEndpointResponse, PluginEndpointError>, String> {
        let bytes = self.invoke_payload(InvokeOperation::HandleEndpoint, request_bytes, context)?;
        serde_json
            ::from_slice(&bytes)
            .map_err(|error| format!("failed to decode worker endpoint response: {error}"))
    }

    fn on_session_closed(
        &self,
        request_bytes: &[u8],
        context: &PluginCallContext
    ) -> Result<Result<(), PluginLifecycleError>, String> {
        let bytes = self.invoke_payload(InvokeOperation::SessionClosed, request_bytes, context)?;
        serde_json
            ::from_slice(&bytes)
            .map_err(|error| format!("failed to decode worker session-closed response: {error}"))
    }

    fn execute_database(
        &self,
        request_bytes: &[u8],
        context: &PluginCallContext
    ) -> Result<
        Result<DatabaseContracts::DatabaseResponse, DatabaseContracts::DatabaseError>,
        String
    > {
        let bytes = self.invoke_payload(InvokeOperation::ExecuteDatabase, request_bytes, context)?;
        serde_json
            ::from_slice(&bytes)
            .map_err(|error| format!("failed to decode worker database response: {error}"))
    }

    fn execute_database_bytes(
        &self,
        request_bytes: &[u8],
        context: &PluginCallContext
    ) -> Result<Vec<u8>, String> {
        self.invoke_payload(InvokeOperation::ExecuteDatabase, request_bytes, context)
    }

    fn on_unload(&self, context: &PluginCallContext) -> Result<PluginLifecycleStatus, String> {
        self.invoke_lifecycle(InvokeOperation::OnUnload, context)
    }

    fn destroy(&self, context: &PluginCallContext) -> Result<PluginLifecycleStatus, String> {
        let status = self.invoke_lifecycle(InvokeOperation::Destroy, context)?;
        if status == PluginLifecycleStatus::Success {
            self.destroyed.store(true, Ordering::Release);
            // The ABI destroy callback has succeeded. Even if the worker fails to acknowledge
            // graceful shutdown, process termination is the final resource-reclamation boundary;
            // do not keep a destroyed plugin registered solely because shutdown acknowledgement failed.
            let _ = self.graceful_shutdown_after_destroy();
        }
        Ok(status)
    }

    fn emergency_shutdown(&self) {
        self.force_stop("runtime requested emergency shutdown");
    }
}

impl Drop for ProcessExecutor {
    fn drop(&mut self) {
        self.force_stop("process executor dropped");
    }
}

pub(super) fn load_plugin(
    path: &Path,
    config: PluginRuntimeConfig
) -> Result<LoadedPlugin, PluginManagerError> {
    let executor = ProcessExecutor::start(path, &config).map_err(|message| {
        PluginManagerError::PluginAbiCall { path: path.to_path_buf(), message }
    })?;
    let executor: Arc<dyn PluginExecutor> = Arc::new(executor);
    let runtime = PluginRuntime::new(executor, config).map_err(|message| {
        PluginManagerError::PluginAbiIncompatible { path: path.to_path_buf(), message }
    })?;

    // Keep the same host-side metadata decoding and consistency checks as InProcessExecutor. The
    // worker's Initialized frame is only a startup acknowledgement, not the registry authority.
    let plugin_name = runtime.name().map_err(|message| PluginManagerError::PluginAbiCall {
        path: path.to_path_buf(),
        message,
    })?;
    let manifest: PluginManifest = runtime
        .manifest()
        .map_err(|message| PluginManagerError::PluginAbiCall {
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

fn unconfined_mode_explicitly_allowed() -> bool {
    std::env
        ::var("EZF_PLUGIN_ALLOW_UNCONFINED")
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

fn configured_duration(
    name: &str,
    default: Duration,
    minimum: Duration,
    maximum: Duration
) -> Duration {
    let value = std::env
        ::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(default);
    value.clamp(minimum, maximum)
}

fn resolve_worker_binary() -> Result<PathBuf, String> {
    if let Some(configured) = std::env::var_os("EZF_PLUGIN_WORKER_PATH") {
        let path = PathBuf::from(configured);
        if !path.is_absolute() {
            return Err("EZF_PLUGIN_WORKER_PATH must be an absolute path".to_owned());
        }
        if !path.is_file() {
            return Err(format!("EZF_PLUGIN_WORKER_PATH does not name a file: {}", path.display()));
        }
        return path
            .canonicalize()
            .map_err(|error| format!("could not canonicalize worker binary path: {error}"));
    }

    let current = std::env
        ::current_exe()
        .map_err(|error| format!("could not locate host executable: {error}"))?;
    let parent = current.parent().ok_or("host executable has no parent directory")?;
    let mut candidates = vec![parent.join(WORKER_BINARY_NAME)];
    if
        parent
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "deps")
    {
        if let Some(target_dir) = parent.parent() {
            candidates.push(target_dir.join(WORKER_BINARY_NAME));
        }
    }
    for candidate in candidates {
        if candidate.is_file() {
            return candidate
                .canonicalize()
                .map_err(|error| format!("could not canonicalize worker binary path: {error}"));
        }
    }
    Err(
        format!(
            "could not find {WORKER_BINARY_NAME} beside the host executable; set EZF_PLUGIN_WORKER_PATH to its absolute path"
        )
    )
}

fn accept_worker(
    listener: &UnixListener,
    supervisor: &WorkerSupervisor
) -> Result<UnixStream, String> {
    let deadline = Instant::now() + SOCKET_STARTUP_TIMEOUT;
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream
                    .set_nonblocking(false)
                    .map_err(|error| {
                        format!(
                            "could not switch accepted worker IPC socket to blocking mode: {error}"
                        )
                    })?;
                return Ok(stream);
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => {
                return Err(format!("could not accept worker IPC connection: {error}"));
            }
        }
        supervisor
            .ensure_alive()
            .map_err(|error| format!("worker exited before connecting: {error}"))?;
        if Instant::now() >= deadline {
            return Err(
                format!(
                    "worker did not connect to its private IPC socket within {:?}",
                    SOCKET_STARTUP_TIMEOUT
                )
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn startup_round_trip<T: serde::Serialize>(
    stream: &mut UnixStream,
    outgoing_type: MessageType,
    request_id: u64,
    control: &T,
    expected_type: MessageType,
    limits: FrameLimits
) -> Result<IpcFrame, String> {
    let frame = IpcFrame::with_control(outgoing_type, request_id, control).map_err(|error|
        format!("could not encode startup {:?}: {error}", outgoing_type)
    )?;
    frame
        .write_to(stream, limits)
        .map_err(|error| format!("could not send startup {:?}: {error}", outgoing_type))?;
    let response = IpcFrame::read_from(stream, limits).map_err(|error|
        format!("could not read startup response to {:?}: {error}", outgoing_type)
    )?;
    ensure_response_type(&response, expected_type, request_id)?;
    if expected_type == MessageType::SandboxReady {
        let sandbox: SandboxReadyControl = response
            .decode_control()
            .map_err(|error| format!("invalid SandboxReady control: {error}"))?;
        if sandbox.profile != SandboxProfile::DevelopmentUnconfined || sandbox.sandbox_enforced {
            return Err(
                "worker returned an unexpected sandbox status for DevelopmentUnconfined mode".to_owned()
            );
        }
    }
    Ok(response)
}

fn ensure_response_type(
    frame: &IpcFrame,
    expected_type: MessageType,
    request_id: u64
) -> Result<(), String> {
    if frame.request_id != request_id {
        return Err(
            format!(
                "worker startup correlation failure: expected request_id {request_id}, got {}",
                frame.request_id
            )
        );
    }
    if frame.message_type == MessageType::ProtocolError {
        let error = frame
            .decode_control::<ProtocolErrorControl>()
            .map_err(|decode| format!("worker returned an invalid ProtocolError: {decode}"))?;
        return Err(format!("worker rejected startup with {:?}: {}", error.code, error.message));
    }
    if frame.message_type != expected_type {
        return Err(
            format!(
                "worker startup protocol violation: expected {:?}, got {:?}",
                expected_type,
                frame.message_type
            )
        );
    }
    Ok(())
}

fn reader_loop(
    mut stream: UnixStream,
    limits: FrameLimits,
    sender: mpsc::Sender<ReaderEvent>,
    supervisor: Arc<WorkerSupervisor>
) {
    loop {
        match IpcFrame::read_from(&mut stream, limits) {
            Ok(frame) => {
                if sender.send(ReaderEvent::Frame(frame)).is_err() {
                    let _ = supervisor.wait_for_exit(SOCKET_STARTUP_TIMEOUT);
                    return;
                }
            }
            Err(error) => {
                // The reader also watches for idle worker exit. It does not invent a restart: it
                // reports disconnect and drives the one shared Child-reaping path to completion.
                let _ = sender.send(ReaderEvent::Disconnected(error.to_string()));
                let _ = supervisor.wait_for_exit(SOCKET_STARTUP_TIMEOUT);
                return;
            }
        }
    }
}

fn log_worker_message(level: PluginIpc::LogLevel, message: &str) {
    match level {
        PluginIpc::LogLevel::Error => log::error!("plugin worker: {}", message),
        PluginIpc::LogLevel::Warn => log::warn!("plugin worker: {}", message),
        PluginIpc::LogLevel::Info => log::info!("plugin worker: {}", message),
        PluginIpc::LogLevel::Debug => log::debug!("plugin worker: {}", message),
        PluginIpc::LogLevel::Trace => log::trace!("plugin worker: {}", message),
    }
}
