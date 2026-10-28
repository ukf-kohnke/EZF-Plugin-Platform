//! Process-backed adapter for `PluginExecutor`.
//!
//! The process executor runs one plugin request at a time per worker and owns/reaps the child.
//! Worker-originated database capabilities are dispatched to a bounded host broker, while logs
//! travel over IPC with host-side plugin identity attached. Calls that exceed their deadline
//! request cooperative cancellation and force-terminate/reap the worker after a bounded grace
//! period. There is no automatic retry policy.

use super::{ PluginCallContext, PluginCancellationSource, PluginExecutor, PluginLifecycleStatus };
use super::super::runtime::PluginExecutorKind;
use super::super::{ LoadedPlugin, PluginRuntime, PluginRuntimeConfig };
use crate::Factory::PluginManagerError;
use crate::PluginManager::executor::PluginCancellationToken;
use PluginAbi::{
    AbiBuffer,
    AbiSlice,
    PluginCancellationV2,
    PLUGIN_ABI_VERSION_V2,
    PLUGIN_STATUS_OK,
};
use PluginContracts::ApiContract::{
    PluginEndpointError,
    PluginEndpointResponse,
    PluginLifecycleError,
    PluginManifest,
    PluginKind,
};
use PluginIpc::{
    CancelControl,
    CapabilityKind,
    CapabilityRequestControl,
    CapabilityResultControl,
    FrameLimits,
    HelloControl,
    InitializeControl,
    InitializedControl,
    InvokeControl,
    InvokeOperation,
    IpcErrorCode,
    IpcFrame,
    LoadPluginControl,
    LogControl,
    LogLevel,
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
use std::ffi::OsString;
use std::sync::atomic::{ AtomicBool, AtomicU64, AtomicUsize, Ordering };
use std::sync::{ mpsc, Mutex };
use std::thread::{ self, JoinHandle };
use std::time::{ Duration, Instant };
use std::{ fs, io };
use std::process::{ Child, Command, ExitStatus, Stdio };
use std::sync::Arc;
use std::os::unix::fs::{ DirBuilderExt, PermissionsExt };
use std::os::unix::process::ExitStatusExt;
use std::net::Shutdown;
use std::os::unix::net::{ UnixListener, UnixStream };

const SOCKET_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const RECEIVE_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DEFAULT_CANCEL_GRACE: Duration = Duration::from_millis(500);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const WORKER_BINARY_NAME: &str = "ezf-plugin-worker";
const MAX_DATABASE_CAPABILITY_BYTES: usize = 1024 * 1024;
const MAX_CAPABILITY_REQUESTS_PER_CALL: u32 = 32;
const MAX_LOG_MESSAGES_PER_CALL: u32 = 256;
const MAX_LOG_MESSAGE_BYTES: usize = 16 * 1024;

static SOCKET_DIRECTORY_COUNTER: AtomicU64 = AtomicU64::new(1);

struct WorkerSupervisor {
    child: Mutex<Option<Child>>,
    exit_status: Mutex<Option<WorkerExit>>,
    terminated: AtomicBool,
    active_calls: AtomicUsize,
    pid: u32,
    started_at: Instant,
}

#[derive(Clone, Copy, Debug)]
struct WorkerExit {
    success: bool,
    code: Option<i32>,
    signal: Option<i32>,
    elapsed: Duration,
    forced: bool,
}

struct OpenCallGuard<'a>(&'a AtomicUsize);

impl Drop for OpenCallGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct ActiveRequestGuard<'a> {
    active_request_id: &'a AtomicU64,
    request_id: u64,
}

impl Drop for ActiveRequestGuard<'_> {
    fn drop(&mut self) {
        let _ = self.active_request_id.compare_exchange(
            self.request_id,
            0,
            Ordering::AcqRel,
            Ordering::Acquire
        );
    }
}

impl WorkerSupervisor {
    fn new(child: Child) -> Self {
        let pid = child.id();
        Self {
            child: Mutex::new(Some(child)),
            exit_status: Mutex::new(None),
            terminated: AtomicBool::new(false),
            active_calls: AtomicUsize::new(0),
            pid,
            started_at: Instant::now(),
        }
    }

    /// Tracks executor calls from before they wait for the single-flight IPC gate. Thus a child
    /// exit report includes both the current wire request and calls blocked behind it.
    fn track_call(&self) -> OpenCallGuard<'_> {
        self.active_calls.fetch_add(1, Ordering::AcqRel);
        OpenCallGuard(&self.active_calls)
    }

    fn ensure_alive(&self) -> Result<(), String> {
        match self.poll_exit()? {
            None => Ok(()),
            Some(_) => Err(self.exit_description()),
        }
    }

    /// `Some(success)` means the child has exited and its status has been collected/reaped.
    fn poll_exit(&self) -> Result<Option<bool>, String> {
        let mut child_guard = self.child.lock().unwrap_or_else(|p| p.into_inner());
        let Some(child) = child_guard.as_mut() else {
            let result = *self.exit_status.lock().unwrap_or_else(|p| p.into_inner());
            return Ok(Some(result.map(|status| status.success).unwrap_or(false)));
        };

        match child.try_wait() {
            Ok(None) => Ok(None),
            Ok(Some(status)) => {
                *child_guard = None;
                let exit = self.record_exit(status, false, None);
                Ok(Some(exit.success))
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
    /// Holding the `Child` mutex makes this the sole kill/wait path even when a timeout, a reader
    /// disconnect and runtime teardown race with one another.
    fn terminate_and_reap(&self, reason: Option<&str>) -> Option<bool> {
        let mut child_guard = self.child.lock().unwrap_or_else(|p| p.into_inner());
        let Some(child) = child_guard.as_mut() else {
            self.terminated.store(true, Ordering::Release);
            let exit = *self.exit_status.lock().unwrap_or_else(|p| p.into_inner());
            return exit.map(|status| status.success);
        };

        let (status, forced) = match child.try_wait() {
            Ok(Some(status)) => (Some(status), false),
            Ok(None) => {
                let kill_succeeded = child.kill().is_ok();
                match child.wait() {
                    Ok(status) => (Some(status), kill_succeeded),
                    Err(error) => {
                        log::error!(
                            "could not reap plugin worker pid={} after kill request: {}; reason={:?}",
                            self.pid,
                            error,
                            reason
                        );
                        (None, kill_succeeded)
                    }
                }
            }
            Err(poll_error) => {
                log::warn!(
                    "could not poll plugin worker pid={} before termination: {}; requesting kill; reason={:?}",
                    self.pid,
                    poll_error,
                    reason
                );
                let kill_succeeded = child.kill().is_ok();
                match child.wait() {
                    Ok(status) => (Some(status), kill_succeeded),
                    Err(error) => {
                        log::error!(
                            "could not reap plugin worker pid={} after poll and kill errors: {}; reason={:?}",
                            self.pid,
                            error,
                            reason
                        );
                        (None, kill_succeeded)
                    }
                }
            }
        };
        match status {
            Some(status) => {
                // Only clear the Child after `try_wait`/`wait` returned an ExitStatus. If wait
                // fails, retain the handle so a later teardown path can retry instead of losing
                // the only ability to reap the process.
                *child_guard = None;
                Some(self.record_exit(status, forced, reason).success)
            }
            None => {
                self.terminated.store(false, Ordering::Release);
                None
            }
        }
    }

    fn record_exit(&self, status: ExitStatus, forced: bool, reason: Option<&str>) -> WorkerExit {
        let exit = WorkerExit {
            success: status.success(),
            code: status.code(),
            signal: status.signal(),
            elapsed: self.started_at.elapsed(),
            forced,
        };
        *self.exit_status.lock().unwrap_or_else(|p| p.into_inner()) = Some(exit);
        self.terminated.store(true, Ordering::Release);
        let open_calls = self.active_calls.load(Ordering::Acquire);
        if forced || !exit.success {
            log::warn!(
                "plugin worker exited: pid={} success={} exit_code={:?} signal={:?} runtime_ms={} open_calls={} forced={} restart_count=0 restart_policy=disabled reason={:?}",
                self.pid,
                exit.success,
                exit.code,
                exit.signal,
                exit.elapsed.as_millis(),
                open_calls,
                exit.forced,
                reason
            );
        } else {
            log::info!(
                "plugin worker exited cleanly: pid={} exit_code={:?} runtime_ms={} open_calls={} restart_count=0 restart_policy=disabled",
                self.pid,
                exit.code,
                exit.elapsed.as_millis(),
                open_calls
            );
        }
        exit
    }

    fn exit_description(&self) -> String {
        let exit = *self.exit_status.lock().unwrap_or_else(|p| p.into_inner());
        match exit {
            Some(exit) =>
                format!(
                    "plugin worker pid={} has exited (success={}, exit_code={:?}, signal={:?}, runtime_ms={}, forced={})",
                    self.pid,
                    exit.success,
                    exit.code,
                    exit.signal,
                    exit.elapsed.as_millis(),
                    exit.forced
                ),
            None => format!("plugin worker pid={} is no longer available", self.pid),
        }
    }
}

impl Drop for WorkerSupervisor {
    fn drop(&mut self) {
        let _ = self.terminate_and_reap(Some("supervisor dropped"));
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

    fn create_plugin_temp_directory(&self) -> Result<PathBuf, String> {
        let path = self.0.join("plugin-tmp");
        fs::DirBuilder
            ::new()
            .mode(0o700)
            .create(&path)
            .map_err(|error| {
                format!("could not create private plugin temp directory: {error}")
            })?;
        fs
            ::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .map_err(|error| {
                format!("could not restrict plugin temp directory permissions: {error}")
            })?;
        path.canonicalize().map_err(|error|
            format!("could not canonicalize plugin temp directory: {error}")
        )
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

struct CapabilityJob {
    request_id: u64,
    parent_request_id: u64,
    operation_id: u64,
    invocation_operation: InvokeOperation,
    capability: CapabilityKind,
    payload: Vec<u8>,
    context: PluginCallContext,
}

struct ProcessExecutor {
    supervisor: Arc<WorkerSupervisor>,
    writer: Arc<Mutex<UnixStream>>,
    events: Mutex<mpsc::Receiver<ReaderEvent>>,
    // The current protocol has one in-flight top-level invocation per worker process. This gate
    // serializes calls even when the host runtime admits/schedules more of them. Raising
    // `max_concurrent_calls` or `worker_threads` therefore does not raise process-mode plugin
    // parallelism; that requires a pool of worker processes or a multiplexed execution protocol.
    call_gate: Mutex<()>,
    reader_thread: Mutex<Option<JoinHandle<()>>>,
    socket_directory: SocketDirectory,
    next_request_id: AtomicU64,
    active_request_id: Arc<AtomicU64>,
    limits: FrameLimits,
    cancellation_grace: Duration,
    shutdown_timeout: Duration,
    destroyed: AtomicBool,
    plugin_name: String,
    plugin_kind: PluginKind,
    capability_sender: mpsc::SyncSender<CapabilityJob>,
}

impl ProcessExecutor {
    fn start(plugin_path: &Path, _config: &PluginRuntimeConfig) -> Result<Self, String> {
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
        let sandbox_launch = configured_sandbox_launch(&plugin_path)?;
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

        let mut command = Command::new(&worker_binary);
        command.arg("--ipc-socket").arg(&socket_path);
        match sandbox_launch.profile {
            SandboxProfile::DevelopmentUnconfined => {
                command.arg("--allow-unconfined");
            }
            SandboxProfile::DomainPlugin | SandboxProfile::DataSourcePlugin => {
                command
                    .arg("--sandbox-profile")
                    .arg(sandbox_profile_argument(sandbox_launch.profile));
                let temp_directory = socket_directory.create_plugin_temp_directory()?;
                command.arg("--plugin-temp-dir").arg(&temp_directory);
                for path in &sandbox_launch.extra_read_paths {
                    command.arg("--extra-read-path").arg(path);
                }
                for path in &sandbox_launch.library_paths {
                    command.arg("--extra-library-path").arg(path);
                }
                if sandbox_launch.profile == SandboxProfile::DataSourcePlugin {
                    let ports = sandbox_launch.datasource_tcp_ports
                        .iter()
                        .map(u16::to_string)
                        .collect::<Vec<_>>()
                        .join(",");
                    command
                        .arg("--datasource-tcp-ports")
                        .arg(ports)
                        .arg("--datasource-egress-policy-acknowledged");
                }
                // Enforced workers do not inherit backend secrets, proxy settings, loader overrides,
                // or arbitrary open-environment configuration. Only explicitly allowlisted names
                // are copied across this process boundary.
                command.env_clear().current_dir(plugin_directory);
                for (name, value) in &sandbox_launch.worker_environment {
                    command.env(name, value);
                }
                if !sandbox_launch.library_paths.is_empty() {
                    let paths = std::env
                        ::join_paths(&sandbox_launch.library_paths)
                        .map_err(|error|
                            format!("could not encode trusted worker library paths: {error}")
                        )?;
                    // This is assembled only from canonical, operator-configured read-only paths;
                    // arbitrary inherited LD_LIBRARY_PATH/LD_PRELOAD values are never propagated.
                    command.env("LD_LIBRARY_PATH", paths);
                }
                command.env("TMPDIR", &temp_directory);
                command.env("HOME", &temp_directory);
            }
        }
        command.stdin(Stdio::null()).stdout(Stdio::null());
        if sandbox_launch.profile == SandboxProfile::DevelopmentUnconfined {
            command.stderr(Stdio::inherit());
        } else {
            // An enforced worker must not inherit the backend's stderr/log file descriptor. Setup
            // diagnostics are returned through ProtocolError and exit telemetry is host-owned.
            command.stderr(Stdio::null());
        }
        let child = command
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
            None,
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
        let sandbox_status_frame = startup_round_trip(
            &mut stream,
            MessageType::Initialize,
            2,
            &(InitializeControl {
                protocol_major: IPC_MAJOR_VERSION,
                protocol_minor: IPC_MINOR_VERSION,
                worker_build: format!("PluginHost/{}", env!("CARGO_PKG_VERSION")),
                plugin_path: plugin_path.to_string_lossy().into_owned(),
                plugin_directory: plugin_directory.to_string_lossy().into_owned(),
                // These are the capabilities this IPC implementation can transport. The host
                // still authorizes every database request against the validated manifest kind;
                // a DataSource plugin cannot recurse through the broker.
                allowed_capabilities: vec![CapabilityKind::DatabaseExecute, CapabilityKind::Log],
                // The child is deliberately configured for one call at a time. The runtime's
                // `max_concurrent_calls` is an admission bound and must not be mistaken for
                // effective process-mode execution parallelism.
                runtime_limits: RuntimeLimits {
                    max_concurrent_calls: 1,
                    max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES as u32,
                    cancellation_grace_ms: cancellation_grace
                        .as_millis()
                        .min(u64::MAX as u128) as u64,
                },
                sandbox_profile: sandbox_launch.profile,
            }),
            MessageType::SandboxReady,
            Some(sandbox_launch.profile),
            limits
        )?;
        let sandbox_status: SandboxReadyControl = sandbox_status_frame
            .decode_control()
            .map_err(|error| format!("invalid SandboxReady control: {error}"))?;
        if sandbox_launch.profile == SandboxProfile::DevelopmentUnconfined {
            log::warn!(
                "starting plugin worker in DevelopmentUnconfined mode; no OS sandbox is active"
            );
        } else {
            log::info!(
                "plugin worker sandbox profile {:?} active features: {}",
                sandbox_launch.profile,
                sandbox_status.active_features.join(",")
            );
            for missing in &sandbox_status.missing_features {
                log::warn!("plugin worker sandbox requires deployment control: {missing}");
            }
            if sandbox_launch.profile == SandboxProfile::DataSourcePlugin {
                log::warn!(
                    "DataSourcePlugin TCP port restrictions do not enforce destination IP allowlists; deployment firewall/container egress policy must be active"
                );
            }
        }
        // No plugin request is dispatched until SandboxReady has been validated and the worker
        // has confirmed the selected policy before loading plugin code.
        let load = IpcFrame::with_control(
            MessageType::LoadPlugin,
            3,
            &(LoadPluginControl { expected_abi_version: PLUGIN_ABI_VERSION_V2 })
        ).map_err(|error| error.to_string())?;
        load.write_to(&mut stream, limits).map_err(|error| error.to_string())?;
        let initialized_frame = loop {
            let frame = IpcFrame::read_from(&mut stream, limits).map_err(|error|
                format!("worker failed while loading plugin: {error}")
            )?;
            if frame.message_type == MessageType::Log {
                match frame.decode_control::<LogControl>() {
                    Ok(log) => log_worker_message("plugin-startup", log.level, &log.message),
                    Err(error) =>
                        log::warn!("plugin worker sent malformed startup Log control: {error}"),
                }
                continue;
            }
            break frame;
        };
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
        let manifest: PluginManifest = serde_json
            ::from_str(&initialized.manifest_json)
            .map_err(|error| format!("worker returned invalid manifest JSON: {error}"))?;
        if !manifest.name.eq_ignore_ascii_case(&initialized.plugin_name) {
            return Err("worker plugin name and manifest name do not match".to_owned());
        }
        if manifest.name.trim().is_empty() {
            return Err("worker returned a manifest with an empty plugin name".to_owned());
        }
        let plugin_name = manifest.name.clone();
        let plugin_kind = manifest.kind;
        match (sandbox_launch.profile, plugin_kind) {
            | (SandboxProfile::DomainPlugin, PluginKind::Domain)
            | (SandboxProfile::DataSourcePlugin, PluginKind::DataSource)
            | (SandboxProfile::DevelopmentUnconfined, _) => {}
            (profile, kind) => {
                return Err(
                    format!(
                        "plugin manifest kind {kind:?} does not match configured sandbox profile {profile:?}; worker will be terminated"
                    )
                );
            }
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
        let reader_disconnect_grace = cancellation_grace;
        let reader_thread = thread::Builder
            ::new()
            .name(format!("ezf-plugin-ipc-reader-{}", std::process::id()))
            .spawn(move ||
                reader_loop(
                    reader,
                    limits,
                    event_sender,
                    reader_supervisor,
                    reader_disconnect_grace
                )
            )
            .map_err(|error| format!("could not start worker IPC reader: {error}"))?;

        let writer = Arc::new(Mutex::new(stream));
        let (capability_sender, capability_receiver) = mpsc::sync_channel::<CapabilityJob>(8);
        let broker_writer = Arc::clone(&writer);
        let broker_active_request_id = Arc::new(AtomicU64::new(0));
        let capability_active_request_id = Arc::clone(&broker_active_request_id);
        let broker_plugin_name = plugin_name.clone();
        let _capability_thread = thread::Builder
            ::new()
            .name(format!("ezf-plugin-capability-broker-{}", std::process::id()))
            .spawn(move ||
                capability_broker_loop(
                    capability_receiver,
                    broker_writer,
                    broker_plugin_name,
                    plugin_kind,
                    capability_active_request_id,
                    limits
                )
            )
            .map_err(|error| format!("could not start capability broker dispatcher: {error}"))?;
        Ok(Self {
            supervisor,
            writer,
            events: Mutex::new(event_receiver),
            call_gate: Mutex::new(()),
            reader_thread: Mutex::new(Some(reader_thread)),
            socket_directory,
            next_request_id: AtomicU64::new(100),
            active_request_id: broker_active_request_id,
            limits,
            cancellation_grace,
            shutdown_timeout,
            destroyed: AtomicBool::new(false),
            plugin_name,
            plugin_kind,
            capability_sender,
        })
    }

    fn invoke(
        &self,
        operation: InvokeOperation,
        payload: &[u8],
        context: &PluginCallContext
    ) -> Result<InvocationResponse, String> {
        // Count calls before acquiring the single-flight gate. If the child exits while one call is
        // executing, the supervisor diagnostics then include every caller queued behind it too.
        let _open_call = self.supervisor.track_call();
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

        // This lock is intentionally held across the full IPC exchange: the current worker protocol
        // is single-flight, not multiplexed. Calls admitted behind it consume runtime admission and
        // host-side worker resources while waiting, but only one plugin operation runs in the child.
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        self.active_request_id.store(request_id, Ordering::Release);
        let _active_request = ActiveRequestGuard {
            active_request_id: self.active_request_id.as_ref(),
            request_id,
        };
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
        let mut capability_requests = 0u32;
        let mut log_messages = 0u32;
        let mut log_limit_warned = false;
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
                        log_messages = log_messages.saturating_add(1);
                        if log_messages <= MAX_LOG_MESSAGES_PER_CALL {
                            if let Ok(log) = response.decode_control::<LogControl>() {
                                log_worker_message(&self.plugin_name, log.level, &log.message);
                            } else {
                                log::warn!(
                                    "plugin={} sent malformed Log control data",
                                    self.plugin_name
                                );
                            }
                        } else if !log_limit_warned {
                            log_limit_warned = true;
                            log::warn!(
                                "plugin={} exceeded the per-call logging limit ({}); additional worker logs are suppressed",
                                self.plugin_name,
                                MAX_LOG_MESSAGES_PER_CALL
                            );
                        }
                        continue;
                    }
                    if response.message_type == MessageType::CapabilityRequest {
                        capability_requests = capability_requests.saturating_add(1);
                        if capability_requests > MAX_CAPABILITY_REQUESTS_PER_CALL {
                            self.force_stop("worker exceeded per-call capability request limit");
                            return Err(
                                format!(
                                    "plugin {} exceeded the per-call capability request limit ({MAX_CAPABILITY_REQUESTS_PER_CALL})",
                                    self.plugin_name
                                )
                            );
                        }
                        if
                            let Err(message) = self.dispatch_capability_request(
                                response,
                                request_id,
                                context.operation_id(),
                                operation,
                                context
                            )
                        {
                            self.force_stop("invalid or unserviceable capability request");
                            return Err(message);
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
                            if operation == InvokeOperation::Manifest {
                                let manifest: PluginManifest = serde_json
                                    ::from_slice(&response.payload)
                                    .map_err(|error|
                                        format!(
                                            "worker returned invalid runtime manifest JSON: {error}"
                                        )
                                    )?;
                                if
                                    !manifest.name.eq_ignore_ascii_case(&self.plugin_name) ||
                                    manifest.kind != self.plugin_kind
                                {
                                    self.force_stop(
                                        "plugin runtime manifest changed after capability policy was bound"
                                    );
                                    return Err(
                                        format!(
                                            "plugin runtime manifest identity/policy changed: startup name={:?} kind={:?}, runtime name={:?} kind={:?}",
                                            self.plugin_name,
                                            self.plugin_kind,
                                            manifest.name,
                                            manifest.kind
                                        )
                                    );
                                }
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

    /// Validates and enqueues worker-originated capabilities. Database work runs on a bounded,
    /// persistent dispatcher thread so the invocation loop can continue handling Cancel/Log and
    /// the IPC reader never waits for database execution.
    fn dispatch_capability_request(
        &self,
        request: IpcFrame,
        invocation_request_id: u64,
        operation_id: u64,
        invocation_operation: InvokeOperation,
        context: &PluginCallContext
    ) -> Result<(), String> {
        if
            request.request_id == 0 ||
            request.request_id == invocation_request_id ||
            request.correlation_id != Some(invocation_request_id)
        {
            return Err(
                format!(
                    "worker capability correlation violation: expected parent request_id {}, got request_id={} correlation_id={:?}",
                    invocation_request_id,
                    request.request_id,
                    request.correlation_id
                )
            );
        }
        let control: CapabilityRequestControl = request
            .decode_control()
            .map_err(|error| {
                format!("worker sent malformed CapabilityRequest control: {error}")
            })?;
        if control.operation_id != operation_id {
            return Err(
                format!(
                    "worker capability operation mismatch: expected operation_id {operation_id}, got {}",
                    control.operation_id
                )
            );
        }

        let job = CapabilityJob {
            request_id: request.request_id,
            parent_request_id: invocation_request_id,
            operation_id,
            invocation_operation,
            capability: control.capability,
            payload: request.payload,
            context: context.clone(),
        };
        match self.capability_sender.try_send(job) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(job)) => {
                send_capability_result(
                    &self.writer,
                    job.request_id,
                    job.parent_request_id,
                    CapabilityResultControl {
                        success: false,
                        error_code: Some(IpcErrorCode::ResourceLimitExceeded),
                        error_message: Some("host capability dispatcher queue is full".to_owned()),
                    },
                    Vec::new(),
                    self.limits
                )
            }
            Err(mpsc::TrySendError::Disconnected(job)) => {
                send_capability_result(
                    &self.writer,
                    job.request_id,
                    job.parent_request_id,
                    CapabilityResultControl {
                        success: false,
                        error_code: Some(IpcErrorCode::WorkerExited),
                        error_message: Some("host capability dispatcher is unavailable".to_owned()),
                    },
                    Vec::new(),
                    self.limits
                )
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
                        log_worker_message(&self.plugin_name, log.level, &log.message);
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
            let _ = self.supervisor.terminate_and_reap(
                Some("graceful shutdown did not complete cleanly")
            );
        }
        self.close_transport_and_join_reader();
        self.socket_directory.cleanup();
        graceful
    }

    fn force_stop(&self, reason: &str) {
        if !self.supervisor.terminated.load(Ordering::Acquire) {
            log::warn!("stopping plugin worker: {reason}");
        }
        let _ = self.supervisor.terminate_and_reap(Some(reason));
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
    fn kind(&self) -> PluginExecutorKind {
        PluginExecutorKind::Process
    }

    fn worker_pid(&self) -> Option<u32> {
        (!self.supervisor.terminated.load(Ordering::Acquire)).then_some(self.supervisor.pid)
    }

    fn validate(&self) -> Result<(), String> {
        self.supervisor.ensure_alive()?;
        if self.destroyed.load(Ordering::Acquire) {
            return Err("process plugin has already been destroyed".to_owned());
        }
        Ok(())
    }

    fn validate_for_lifecycle(&self) -> Result<(), String> {
        // If the ABI destroy callback completed but the shutdown path could not confirm reaping,
        // retain the Child handle and retry the one serialized kill/wait path on the next unload
        // attempt. Do not detach a possibly-live worker just because the instance is already
        // marked destroyed.
        if
            self.destroyed.load(Ordering::Acquire) &&
            !self.supervisor.terminated.load(Ordering::Acquire)
        {
            log::warn!(
                "retrying process termination/reaping for destroyed plugin worker pid={}",
                self.supervisor.pid
            );
            let _ = self.supervisor.terminate_and_reap(
                Some("retrying teardown after incomplete graceful shutdown")
            );
            self.close_transport_and_join_reader();
            self.socket_directory.cleanup();
        }
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
        match self.invoke_lifecycle(InvokeOperation::OnUnload, context) {
            Err(error) if self.supervisor.terminated.load(Ordering::Acquire) => {
                log::warn!(
                    "plugin worker exit/reaping was confirmed before graceful on_unload completion: {}",
                    error
                );
                self.destroyed.store(true, Ordering::Release);
                Ok(PluginLifecycleStatus::ForcedTermination)
            }
            result => result,
        }
    }

    fn destroy(&self, context: &PluginCallContext) -> Result<PluginLifecycleStatus, String> {
        let status = match self.invoke_lifecycle(InvokeOperation::Destroy, context) {
            Ok(status) => status,
            Err(error) if self.supervisor.terminated.load(Ordering::Acquire) => {
                log::warn!(
                    "plugin worker exit/reaping was confirmed before graceful destroy completion: {}",
                    error
                );
                self.destroyed.store(true, Ordering::Release);
                return Ok(PluginLifecycleStatus::ForcedTermination);
            }
            Err(error) => {
                return Err(error);
            }
        };
        if status == PluginLifecycleStatus::Success {
            self.destroyed.store(true, Ordering::Release);
            // The ABI destroy callback has succeeded. Even if the worker fails to acknowledge
            // graceful shutdown, process termination is the final resource-reclamation boundary;
            // do not keep a destroyed plugin registered solely because shutdown acknowledgement failed.
            if !self.graceful_shutdown_after_destroy() {
                if self.supervisor.terminated.load(Ordering::Acquire) {
                    return Ok(PluginLifecycleStatus::ForcedTermination);
                }
                return Err(
                    "graceful worker shutdown failed and process termination/reaping could not be confirmed; unload may be retried".to_owned()
                );
            }
        }
        Ok(status)
    }

    fn lifecycle_escalation_grace(&self) -> Duration {
        // Allow the executor's cooperative Cancel and bounded kill/reap path to report back after
        // the runtime has cancelled the lifecycle context at its deadline.
        self.cancellation_grace.saturating_add(Duration::from_millis(250))
    }

    fn process_exit_confirmed(&self) -> bool {
        self.supervisor.terminated.load(Ordering::Acquire)
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

#[derive(Debug)]
struct SandboxLaunchConfig {
    profile: SandboxProfile,
    extra_read_paths: Vec<PathBuf>,
    library_paths: Vec<PathBuf>,
    datasource_tcp_ports: Vec<u16>,
    worker_environment: Vec<(String, OsString)>,
}

fn configured_sandbox_launch(plugin_path: &Path) -> Result<SandboxLaunchConfig, String> {
    let path_key = plugin_path.to_string_lossy().into_owned();
    let mapping_raw = std::env::var("EZF_PLUGIN_SANDBOX_PROFILES").ok();
    let selected = if let Some(raw) = mapping_raw.as_ref() {
        let value: serde_json::Value = serde_json
            ::from_str(raw)
            .map_err(|error| {
                format!(
                    "EZF_PLUGIN_SANDBOX_PROFILES must be a JSON object keyed by canonical absolute plugin path: {error}"
                )
            })?;
        let mapping = value.as_object().ok_or("EZF_PLUGIN_SANDBOX_PROFILES must be a JSON object")?;
        let value = mapping
            .get(&path_key)
            .ok_or_else(|| {
                format!(
                    "no sandbox profile configured for plugin path {path_key}; add this canonical path to EZF_PLUGIN_SANDBOX_PROFILES"
                )
            })?;
        let value = value
            .as_str()
            .ok_or_else(|| { format!("sandbox profile value for {path_key} must be a string") })?;
        Some(parse_sandbox_profile(value)?)
    } else {
        std::env
            ::var("EZF_PLUGIN_SANDBOX_PROFILE")
            .ok()
            .map(|value| parse_sandbox_profile(&value))
            .transpose()?
    };
    let profile = match selected {
        Some(profile) => profile,
        None if unconfined_mode_explicitly_allowed() => SandboxProfile::DevelopmentUnconfined,
        None => {
            return Err(
                "process plugin execution requires EZF_PLUGIN_SANDBOX_PROFILE=domain_plugin (Linux enforced mode), a per-plugin EZF_PLUGIN_SANDBOX_PROFILES mapping, or the explicit development opt-in EZF_PLUGIN_ALLOW_UNCONFINED=1".into()
            );
        }
    };

    if profile == SandboxProfile::DevelopmentUnconfined {
        if !unconfined_mode_explicitly_allowed() {
            return Err(
                "DevelopmentUnconfined requires the explicit EZF_PLUGIN_ALLOW_UNCONFINED=1 opt-in".into()
            );
        }
        return Ok(SandboxLaunchConfig {
            profile,
            extra_read_paths: Vec::new(),
            library_paths: Vec::new(),
            datasource_tcp_ports: Vec::new(),
            worker_environment: Vec::new(),
        });
    }
    if !cfg!(target_os = "linux") {
        return Err(
            format!(
                "sandbox profile {profile:?} is enforced only on Linux; this platform must remain in explicit DevelopmentUnconfined mode"
            )
        );
    }

    let mut library_paths = Vec::new();
    if let Some(raw) = std::env::var_os("EZF_PLUGIN_SANDBOX_LIBRARY_PATHS") {
        for path in std::env::split_paths(&raw) {
            if path.as_os_str().is_empty() {
                continue;
            }
            let canonical = path
                .canonicalize()
                .map_err(|error| {
                    format!(
                        "configured sandbox library path {} does not exist or cannot be canonicalized: {error}",
                        path.display()
                    )
                })?;
            if !canonical.is_dir() || canonical == Path::new("/") {
                return Err(
                    format!(
                        "sandbox library path must be a specific directory, not {}",
                        canonical.display()
                    )
                );
            }
            for forbidden in ["/proc", "/sys", "/dev", "/run"] {
                if canonical == Path::new(forbidden) || canonical.starts_with(forbidden) {
                    return Err(
                        format!(
                            "configured sandbox library path {} is not permitted",
                            canonical.display()
                        )
                    );
                }
            }
            for broad_root in ["/etc", "/home", "/root", "/var", "/tmp"] {
                if canonical == Path::new(broad_root) {
                    return Err(
                        format!(
                            "sandbox library path {broad_root} is too broad; configure a specific library directory"
                        )
                    );
                }
            }
            let metadata = fs
                ::metadata(&canonical)
                .map_err(|error|
                    format!("could not inspect library directory {}: {error}", canonical.display())
                )?;
            if (metadata.permissions().mode() & 0o022) != 0 {
                return Err(
                    format!(
                        "sandbox library directory {} must not be group- or world-writable",
                        canonical.display()
                    )
                );
            }
            library_paths.push(canonical);
        }
    }
    library_paths.sort();
    library_paths.dedup();

    let mut extra_read_paths = library_paths.clone();
    if let Some(raw) = std::env::var_os("EZF_PLUGIN_SANDBOX_READ_PATHS") {
        for path in std::env::split_paths(&raw) {
            if path.as_os_str().is_empty() {
                continue;
            }
            let canonical = path
                .canonicalize()
                .map_err(|error| {
                    format!(
                        "configured sandbox read path {} does not exist or cannot be canonicalized: {error}",
                        path.display()
                    )
                })?;
            if canonical == Path::new("/") {
                return Err(
                    "EZF_PLUGIN_SANDBOX_READ_PATHS may not grant access to filesystem root".into()
                );
            }
            for forbidden in ["/proc", "/sys", "/dev", "/run"] {
                if canonical == Path::new(forbidden) || canonical.starts_with(forbidden) {
                    return Err(
                        format!(
                            "configured sandbox read path {} is not permitted",
                            canonical.display()
                        )
                    );
                }
            }
            for broad_root in ["/etc", "/home", "/root", "/var", "/tmp"] {
                if canonical == Path::new(broad_root) {
                    return Err(
                        format!(
                            "sandbox read path {broad_root} is too broad; configure a specific file or subdirectory"
                        )
                    );
                }
            }
            extra_read_paths.push(canonical);
        }
    }
    extra_read_paths.sort();
    extra_read_paths.dedup();

    let mut datasource_tcp_ports = Vec::new();
    if profile == SandboxProfile::DataSourcePlugin {
        let acknowledged = std::env
            ::var("EZF_PLUGIN_DATASOURCE_EGRESS_POLICY")
            .map(|value| value.trim().eq_ignore_ascii_case("externally-enforced"))
            .unwrap_or(false);
        if !acknowledged {
            return Err(
                "DataSourcePlugin requires EZF_PLUGIN_DATASOURCE_EGRESS_POLICY=externally-enforced because IP-level egress allowlisting must be enforced by the deployment firewall/container".into()
            );
        }
        let raw = std::env
            ::var("EZF_PLUGIN_DATASOURCE_ALLOWED_TCP_PORTS")
            .map_err(|_| {
                "DataSourcePlugin requires EZF_PLUGIN_DATASOURCE_ALLOWED_TCP_PORTS (for example 1433)".to_owned()
            })?;
        for item in raw.split(',') {
            let port = item
                .trim()
                .parse::<u16>()
                .map_err(|_| format!("invalid DataSource TCP port {item:?}"))?;
            if port == 0 {
                return Err("DataSource TCP ports must be between 1 and 65535".into());
            }
            datasource_tcp_ports.push(port);
        }
        datasource_tcp_ports.sort_unstable();
        datasource_tcp_ports.dedup();
        if datasource_tcp_ports.is_empty() || datasource_tcp_ports.len() > 32 {
            return Err("DataSource TCP port allowlist must contain between 1 and 32 ports".into());
        }
    }

    let worker_environment = configured_worker_environment(profile)?;
    Ok(SandboxLaunchConfig {
        profile,
        extra_read_paths,
        library_paths,
        datasource_tcp_ports,
        worker_environment,
    })
}

fn parse_sandbox_profile(value: &str) -> Result<SandboxProfile, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "domain_plugin" | "domain" => Ok(SandboxProfile::DomainPlugin),
        "data_source_plugin" | "datasource" | "data_source" => Ok(SandboxProfile::DataSourcePlugin),
        "development_unconfined" | "unconfined" => Ok(SandboxProfile::DevelopmentUnconfined),
        other =>
            Err(
                format!(
                    "unsupported plugin sandbox profile {other:?}; expected domain_plugin, data_source_plugin, or development_unconfined"
                )
            ),
    }
}

fn sandbox_profile_argument(profile: SandboxProfile) -> &'static str {
    match profile {
        SandboxProfile::DomainPlugin => "domain_plugin",
        SandboxProfile::DataSourcePlugin => "data_source_plugin",
        SandboxProfile::DevelopmentUnconfined => "development_unconfined",
    }
}

fn configured_worker_environment(
    profile: SandboxProfile
) -> Result<Vec<(String, OsString)>, String> {
    let Some(raw) = std::env::var("EZF_PLUGIN_WORKER_ENV_ALLOWLIST").ok() else {
        return Ok(Vec::new());
    };
    let mut result = Vec::new();
    for name in raw
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty()) {
        if
            !name.chars().all(|ch| { ch.is_ascii_alphanumeric() || ch == '_' }) ||
            name.as_bytes()[0].is_ascii_digit()
        {
            return Err(
                format!(
                    "invalid environment variable name in EZF_PLUGIN_WORKER_ENV_ALLOWLIST: {name:?}"
                )
            );
        }
        let upper = name.to_ascii_uppercase();
        if
            upper.starts_with("LD_") ||
            upper.starts_with("DYLD_") ||
            upper.starts_with("EZF_PLUGIN_") ||
            upper == "RUSTFLAGS" ||
            upper == "TMPDIR" ||
            upper == "TMP" ||
            upper == "TEMP" ||
            upper == "HOME" ||
            upper == "PATH"
        {
            return Err(
                format!(
                    "environment variable {name} is not permitted in the plugin worker allowlist"
                )
            );
        }
        if
            profile == SandboxProfile::DomainPlugin &&
            [
                "SECRET",
                "TOKEN",
                "PASSWORD",
                "PASSWD",
                "CREDENTIAL",
                "DATABASE",
                "CONNECTION",
                "DB_",
                "SQL",
                "ODBC",
                "MOLGENDB",
                "PRIVATE_KEY",
                "API_KEY",
            ]
                .iter()
                .any(|marker| upper.contains(marker))
        {
            return Err(
                format!(
                    "secret/database-like environment variable name {name} cannot be passed to a DomainPlugin worker"
                )
            );
        }
        if let Some(value) = std::env::var_os(name) {
            result.push((name.to_owned(), value));
        }
    }
    result.sort_by(|left, right| left.0.cmp(&right.0));
    result.dedup_by(|left, right| left.0 == right.0);
    Ok(result)
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
    expected_sandbox_profile: Option<SandboxProfile>,
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
        let expected = expected_sandbox_profile.ok_or(
            "missing expected sandbox profile for SandboxReady"
        )?;
        if sandbox.profile != expected {
            return Err(
                format!(
                    "worker returned sandbox profile {:?}, expected {:?}",
                    sandbox.profile,
                    expected
                )
            );
        }
        if expected == SandboxProfile::DevelopmentUnconfined {
            if sandbox.sandbox_enforced {
                return Err(
                    "worker incorrectly claimed an OS sandbox for DevelopmentUnconfined mode".to_owned()
                );
            }
        } else {
            if !sandbox.sandbox_enforced {
                return Err(format!("worker did not enforce sandbox profile {:?}", expected));
            }
            for required in [
                "prctl_no_new_privs",
                "dumpable_disabled",
                "effective_capabilities_zero",
                "seccomp_process_creation_and_privilege_syscalls_restricted",
                "seccomp_cross_process_signals_and_capability_mutation_denied",
            ] {
                if !sandbox.active_features.iter().any(|feature| feature == required) {
                    return Err(
                        format!(
                            "worker reported enforced sandbox without required feature {required}"
                        )
                    );
                }
            }
            if !sandbox.active_features.iter().any(|feature| feature.starts_with("landlock_abi_")) {
                return Err(
                    "worker reported enforced sandbox without active Landlock ABI".to_owned()
                );
            }
            let profile_feature = match expected {
                SandboxProfile::DomainPlugin => "seccomp_direct_network_syscalls_denied",
                SandboxProfile::DataSourcePlugin => "landlock_tcp_connect_port_allowlist",
                SandboxProfile::DevelopmentUnconfined => unreachable!(),
            };
            if !sandbox.active_features.iter().any(|feature| feature == profile_feature) {
                return Err(
                    format!(
                        "worker reported enforced profile {:?} without required feature {profile_feature}",
                        expected
                    )
                );
            }
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
    supervisor: Arc<WorkerSupervisor>,
    shutdown_timeout: Duration
) {
    loop {
        match IpcFrame::read_from(&mut stream, limits) {
            Ok(frame) => {
                if sender.send(ReaderEvent::Frame(frame)).is_err() {
                    reap_after_ipc_disconnect(&supervisor, shutdown_timeout);
                    return;
                }
            }
            Err(error) => {
                // Notify the active call before waiting. If the IPC channel closes while idle and
                // the child remains alive, the reader still escalates and reaps it after the
                // bounded cancellation grace. No operation is ever replayed and no automatic restart
                // is performed in this migration phase.
                let _ = sender.send(ReaderEvent::Disconnected(error.to_string()));
                reap_after_ipc_disconnect(&supervisor, shutdown_timeout);
                return;
            }
        }
    }
}

fn reap_after_ipc_disconnect(supervisor: &WorkerSupervisor, grace: Duration) {
    match supervisor.wait_for_exit(grace) {
        Ok(Some(_)) => {}
        Ok(None) => {
            log::warn!(
                "plugin worker pid={} closed IPC but remained alive after {:?}; terminating and reaping",
                supervisor.pid,
                grace
            );
            let _ = supervisor.terminate_and_reap(
                Some("IPC disconnected while child remained alive")
            );
        }
        Err(error) => {
            log::error!(
                "could not inspect plugin worker pid={} after IPC disconnect: {}; terminating and reaping",
                supervisor.pid,
                error
            );
            let _ = supervisor.terminate_and_reap(
                Some("failed to inspect child after IPC disconnect")
            );
        }
    }
}

fn capability_broker_loop(
    receiver: mpsc::Receiver<CapabilityJob>,
    writer: Arc<Mutex<UnixStream>>,
    plugin_name: String,
    plugin_kind: PluginKind,
    active_request_id: Arc<AtomicU64>,
    limits: FrameLimits
) {
    while let Ok(job) = receiver.recv() {
        let (control, payload) = match job.capability {
            CapabilityKind::DatabaseExecute =>
                match
                    execute_database_capability(
                        plugin_kind,
                        &plugin_name,
                        job.invocation_operation,
                        &job.payload,
                        &job.context
                    )
                {
                    Ok(payload) if payload.len() <= limits.max_payload_bytes =>
                        (
                            CapabilityResultControl {
                                success: true,
                                error_code: None,
                                error_message: None,
                            },
                            payload,
                        ),
                    Ok(_) =>
                        (
                            CapabilityResultControl {
                                success: false,
                                error_code: Some(IpcErrorCode::ResourceLimitExceeded),
                                error_message: Some(
                                    format!(
                                        "database capability result exceeded {} bytes",
                                        limits.max_payload_bytes
                                    )
                                ),
                            },
                            Vec::new(),
                        ),
                    Err((code, message)) =>
                        (
                            CapabilityResultControl {
                                success: false,
                                error_code: Some(code),
                                error_message: Some(message),
                            },
                            Vec::new(),
                        ),
                }
            CapabilityKind::Log =>
                (
                    CapabilityResultControl {
                        success: false,
                        error_code: Some(IpcErrorCode::CapabilityDenied),
                        error_message: Some(
                            "logging uses the dedicated Log IPC message".to_owned()
                        ),
                    },
                    Vec::new(),
                ),
        };
        // Never deliver a late DB result to a call that timed out while the broker was working.
        // Send a cancellation result instead so the callback waiting in the worker can unwind.
        let (control, payload) = if
            job.context.is_deadline_expired() ||
            job.context.cancellation_token().is_cancelled()
        {
            (
                CapabilityResultControl {
                    success: false,
                    error_code: Some(IpcErrorCode::Cancelled),
                    error_message: Some(
                        "capability result discarded because its parent call was cancelled or expired".to_owned()
                    ),
                },
                Vec::new(),
            )
        } else {
            (control, payload)
        };
        if active_request_id.load(Ordering::Acquire) != job.parent_request_id {
            log::debug!(
                "plugin={} discarded capability result for inactive parent_request_id={} operation_id={}",
                plugin_name,
                job.parent_request_id,
                job.operation_id
            );
            continue;
        }
        if
            let Err(error) = send_capability_result(
                &writer,
                job.request_id,
                job.parent_request_id,
                control,
                payload,
                limits
            )
        {
            log::warn!(
                "plugin={} failed to send capability result request_id={} operation_id={}: {}",
                plugin_name,
                job.request_id,
                job.operation_id,
                error
            );
        }
    }
}

fn send_capability_result(
    writer: &Arc<Mutex<UnixStream>>,
    request_id: u64,
    parent_request_id: u64,
    control: CapabilityResultControl,
    payload: Vec<u8>,
    limits: FrameLimits
) -> Result<(), String> {
    let mut frame = IpcFrame::with_control(
        MessageType::CapabilityResult,
        request_id,
        &control
    ).map_err(|error| format!("could not encode CapabilityResult: {error}"))?;
    frame.correlation_id = Some(parent_request_id);
    frame.payload = payload;
    let mut writer = writer.lock().unwrap_or_else(|p| p.into_inner());
    frame.write_to(&mut *writer, limits).map_err(|error| error.to_string())
}

fn execute_database_capability(
    plugin_kind: PluginKind,
    plugin_name: &str,
    invocation_operation: InvokeOperation,
    payload: &[u8],
    context: &PluginCallContext
) -> Result<Vec<u8>, (IpcErrorCode, String)> {
    // A DataSource is the terminal database provider. Letting it recurse through the same
    // generic database broker can deadlock the datasource route or create an execution loop.
    if !database_capability_allowed(plugin_kind, invocation_operation) {
        return Err((
            IpcErrorCode::CapabilityDenied,
            format!(
                "database capability denied for {:?} plugin {} during {:?}",
                plugin_kind,
                plugin_name,
                invocation_operation
            ),
        ));
    }
    if payload.len() > MAX_DATABASE_CAPABILITY_BYTES {
        return Err((
            IpcErrorCode::ResourceLimitExceeded,
            format!("database capability payload exceeded {MAX_DATABASE_CAPABILITY_BYTES} bytes"),
        ));
    }
    let request: DatabaseContracts::DatabaseRequest = serde_json
        ::from_slice(payload)
        .map_err(|error| {
            (
                IpcErrorCode::ProtocolViolation,
                format!("invalid database capability request: {error}"),
            )
        })?;
    let statement = match &request {
        DatabaseContracts::DatabaseRequest::Query(query) => query.statement.as_str(),
        DatabaseContracts::DatabaseRequest::Execute(execute) => execute.statement.as_str(),
    };
    if statement.trim().is_empty() {
        return Err((
            IpcErrorCode::CapabilityDenied,
            "database capability requires a non-empty statement".to_owned(),
        ));
    }
    if context.is_deadline_expired() || context.cancellation_token().is_cancelled() {
        return Err((
            IpcErrorCode::Cancelled,
            "database capability was cancelled or expired before execution".to_owned(),
        ));
    }
    let capability = crate::Factory
        ::database_capability()
        .ok_or_else(|| (
            IpcErrorCode::CapabilityDenied,
            "host database executor has not been registered".to_owned(),
        ))?;

    // Extend the upstream cancellation check with the parent call's deadline. This matters when
    // the DB provider is itself another ProcessExecutor: its ABI cancellation callback must see
    // the endpoint deadline even if the endpoint process has already received Cancel.
    let upstream_cancellation = context.cancellation_token();
    let deadline = context.deadline();
    let cancellation_source = PluginCancellationSource::new(
        Some(
            Arc::new(move || { upstream_cancellation.is_cancelled() || Instant::now() >= deadline })
        )
    );
    let token = cancellation_source.token();
    let cancellation = PluginCancellationV2 {
        context: (&token as *const PluginCancellationToken).cast(),
        is_cancelled: process_cancellation_is_cancelled,
    };
    let mut response = AbiBuffer::EMPTY;
    let status = unsafe {
        (capability.execute)(AbiSlice::from_bytes(payload), cancellation, &mut response)
    };
    if status != PLUGIN_STATUS_OK {
        if !response.ptr.is_null() {
            unsafe {
                (capability.free)(response);
            }
        }
        return Err((
            if token.is_cancelled() { IpcErrorCode::Cancelled } else { IpcErrorCode::PluginError },
            format!("host database executor returned ABI status {status}"),
        ));
    }
    let bytes = (unsafe { response.to_vec() }).ok_or_else(|| {
        if !response.ptr.is_null() {
            unsafe {
                (capability.free)(response);
            }
        }
        (
            IpcErrorCode::ProtocolViolation,
            "host database executor returned an invalid response buffer".to_owned(),
        )
    })?;
    unsafe {
        (capability.free)(response);
    }

    let _ = serde_json
        ::from_slice::<
            Result<DatabaseContracts::DatabaseResponse, DatabaseContracts::DatabaseError>
        >(&bytes)
        .map_err(|error| (
            IpcErrorCode::ProtocolViolation,
            format!("host database response was not a valid result envelope: {error}"),
        ))?;

    Ok(bytes)
}

fn database_capability_allowed(kind: PluginKind, operation: InvokeOperation) -> bool {
    // Domain plugins may use the host broker only for productive/session work. DataSource plugins
    // are terminal DB providers and must not recurse through this generic capability; lifecycle
    // metadata and unload callbacks must not trigger database work either. Actual SQL/table rights
    // still belong to the configured database role because DatabaseContracts carries raw SQL.
    kind == PluginKind::Domain &&
        matches!(operation, InvokeOperation::HandleEndpoint | InvokeOperation::SessionClosed)
}

unsafe extern "C" fn process_cancellation_is_cancelled(context: *const std::ffi::c_void) -> bool {
    if context.is_null() {
        return true;
    }
    let token = unsafe { &*(context as *const super::PluginCancellationToken) };
    token.is_cancelled()
}

fn log_worker_message(plugin: &str, level: LogLevel, message: &str) {
    // Plugin identity is supplied by the validated host-side manifest, never by the IPC payload.
    // Truncate again on the host: a modified worker must not bypass the worker-side log limit.
    let bounded = if message.len() > MAX_LOG_MESSAGE_BYTES {
        let mut end = MAX_LOG_MESSAGE_BYTES;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}… [truncated]", &message[..end])
    } else {
        message.to_owned()
    };
    match level {
        LogLevel::Error => log::error!("plugin={} {}", plugin, bounded),
        LogLevel::Warn => log::warn!("plugin={} {}", plugin, bounded),
        LogLevel::Info => log::info!("plugin={} {}", plugin, bounded),
        LogLevel::Debug => log::debug!("plugin={} {}", plugin, bounded),
        LogLevel::Trace => log::trace!("plugin={} {}", plugin, bounded),
    }
}

#[cfg(test)]
mod supervisor_tests {
    use super::*;

    #[test]
    fn database_broker_policy_allows_domain_plugins_only() {
        assert!(database_capability_allowed(PluginKind::Domain, InvokeOperation::HandleEndpoint));
        assert!(database_capability_allowed(PluginKind::Domain, InvokeOperation::SessionClosed));
        assert!(
            !database_capability_allowed(PluginKind::DataSource, InvokeOperation::HandleEndpoint)
        );
        assert!(!database_capability_allowed(PluginKind::Domain, InvokeOperation::Name));
        assert!(!database_capability_allowed(PluginKind::Domain, InvokeOperation::Manifest));
        assert!(!database_capability_allowed(PluginKind::Domain, InvokeOperation::ExecuteDatabase));
        assert!(!database_capability_allowed(PluginKind::Domain, InvokeOperation::OnUnload));
    }

    #[test]
    fn supervisor_observes_and_caches_child_exit_status() {
        let child = Command::new("/bin/sh")
            .args(["-c", "exit 17"])
            .spawn()
            .expect("test shell must start");
        let supervisor = WorkerSupervisor::new(child);

        assert_eq!(
            supervisor
                .wait_for_exit(Duration::from_secs(2))
                .expect("child status must be readable"),
            Some(false)
        );
        assert!(supervisor.terminated.load(Ordering::Acquire));
        assert!(supervisor.exit_description().contains("exit_code=Some(17)"));
        assert_eq!(
            supervisor.poll_exit().expect("cached status must remain readable"),
            Some(false)
        );
        assert!(
            supervisor.child
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_none()
        );
    }

    #[test]
    fn concurrent_termination_requests_share_one_kill_and_reap_path() {
        let child = Command::new("/bin/sleep").arg("30").spawn().expect("test child must start");
        let supervisor = Arc::new(WorkerSupervisor::new(child));
        let first_supervisor = Arc::clone(&supervisor);
        let second_supervisor = Arc::clone(&supervisor);

        let first = thread::spawn(move || {
            first_supervisor.terminate_and_reap(Some("supervisor unit test"))
        });
        let second = thread::spawn(move || {
            second_supervisor.terminate_and_reap(Some("supervisor unit test"))
        });

        assert_eq!(first.join().expect("first termination must not panic"), Some(false));
        assert_eq!(second.join().expect("second termination must not panic"), Some(false));
        assert!(supervisor.terminated.load(Ordering::Acquire));
        let exit = *supervisor.exit_status.lock().unwrap_or_else(|p| p.into_inner());
        assert!(exit.expect("exit status must be cached").forced);
        assert!(
            supervisor.child
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_none()
        );
    }

    #[test]
    fn open_call_count_includes_calls_waiting_for_the_ipc_gate() {
        let child = Command::new("/bin/sleep").arg("30").spawn().expect("test child must start");
        let supervisor = WorkerSupervisor::new(child);
        let first = supervisor.track_call();
        let second = supervisor.track_call();
        assert_eq!(supervisor.active_calls.load(Ordering::Acquire), 2);
        drop(first);
        assert_eq!(supervisor.active_calls.load(Ordering::Acquire), 1);
        drop(second);
        assert_eq!(supervisor.active_calls.load(Ordering::Acquire), 0);
        let _ = supervisor.terminate_and_reap(Some("supervisor unit test cleanup"));
    }
}
