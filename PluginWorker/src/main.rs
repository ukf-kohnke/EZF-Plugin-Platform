//! Process-backed worker with ABI V2 capability callbacks bridged over private IPC.
//!
//! This binary intentionally supports only the explicit DevelopmentUnconfined profile. It is
//! not a production sandbox and must not be treated as one. The worker is single-flight: it
//! executes one plugin callback at a time, while DB callbacks perform a correlated IPC exchange
//! and logs are forwarded to the supervisor.

#[cfg(unix)]
mod unix_worker {
    use PluginAbi::{
        AbiBuffer,
        AbiSlice,
        PluginCancellationV2,
        PluginHostServicesV2,
        PLUGIN_ABI_VERSION_V2,
        PLUGIN_STATUS_INVALID_ARGUMENT,
        PLUGIN_STATUS_OK,
    };
    use DatabaseContracts::{ DatabaseError, DatabaseErrorKind, DatabaseResponse };
    use PluginExecutionCore::{ EndpointCallOutput, PluginLoadError };
    use PluginIpc::{
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
        PongControl,
        ProtocolErrorControl,
        ResultControl,
        SandboxReadyControl,
        ShutdownCompleteControl,
        DEFAULT_MAX_CONTROL_BYTES,
        DEFAULT_MAX_PAYLOAD_BYTES,
        IPC_HEADER_LEN,
        IPC_MAJOR_VERSION,
        IPC_MINOR_VERSION,
    };
    use std::ffi::c_void;
    use std::os::unix::net::UnixStream;
    use std::path::{ Path, PathBuf };
    use std::sync::atomic::{ AtomicBool, AtomicU64, Ordering };
    use std::sync::{ Arc, Mutex, OnceLock, Weak };
    use std::time::Instant;

    const WORKER_BUILD: &str = env!("CARGO_PKG_VERSION");
    const MAX_DATABASE_CAPABILITY_BYTES: usize = 1024 * 1024;
    const MAX_PLUGIN_LOG_BYTES: usize = 16 * 1024;
    const CAPABILITY_REQUEST_ID_BASE: u64 = 1 << 48;

    static WORKER_CHANNEL: OnceLock<WorkerCapabilityChannel> = OnceLock::new();

    /// Local ABI callbacks have no userdata parameter, so this process-wide channel is safe only
    /// because each Worker process loads exactly one plugin. The request gate serializes the full
    /// synchronous capability exchange; the main request loop is blocked inside the ABI callback
    /// while this dedicated reader consumes its matching response.
    struct WorkerCapabilityChannel {
        writer: Mutex<UnixStream>,
        reader: Mutex<UnixStream>,
        request_gate: Mutex<()>,
        next_request_id: AtomicU64,
        active_request_id: AtomicU64,
        active_operation_id: AtomicU64,
        active_cancellation: Mutex<Option<Weak<AtomicBool>>>,
        allowed_capabilities: Mutex<Vec<CapabilityKind>>,
        limits: Mutex<FrameLimits>,
    }

    impl WorkerCapabilityChannel {
        fn new(writer: UnixStream, reader: UnixStream) -> Self {
            Self {
                writer: Mutex::new(writer),
                reader: Mutex::new(reader),
                request_gate: Mutex::new(()),
                next_request_id: AtomicU64::new(CAPABILITY_REQUEST_ID_BASE),
                active_request_id: AtomicU64::new(0),
                active_operation_id: AtomicU64::new(0),
                active_cancellation: Mutex::new(None),
                allowed_capabilities: Mutex::new(Vec::new()),
                limits: Mutex::new(FrameLimits::default()),
            }
        }

        fn set_limits(&self, limits: FrameLimits) {
            *self.limits.lock().unwrap_or_else(|p| p.into_inner()) = limits;
        }

        fn set_capabilities(&self, capabilities: Vec<CapabilityKind>) {
            *self.allowed_capabilities.lock().unwrap_or_else(|p| p.into_inner()) = capabilities;
        }

        fn has_capability(&self, capability: CapabilityKind) -> bool {
            self.allowed_capabilities
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .contains(&capability)
        }

        fn begin_call(&self, request_id: u64, operation_id: u64, cancellation: &Arc<AtomicBool>) {
            *self.active_cancellation.lock().unwrap_or_else(|p| p.into_inner()) = Some(
                Arc::downgrade(cancellation)
            );
            self.active_operation_id.store(operation_id, Ordering::Release);
            self.active_request_id.store(request_id, Ordering::Release);
        }

        fn end_call(&self, request_id: u64) {
            if
                self.active_request_id
                    .compare_exchange(request_id, 0, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                self.active_operation_id.store(0, Ordering::Release);
                *self.active_cancellation.lock().unwrap_or_else(|p| p.into_inner()) = None;
            }
        }

        fn write_frame(&self, frame: &IpcFrame, limits: FrameLimits) -> Result<(), String> {
            let mut writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
            frame.write_to(&mut *writer, limits).map_err(|error| error.to_string())
        }

        fn request_database(
            &self,
            payload: &[u8],
            cancellation: PluginCancellationV2
        ) -> Result<Vec<u8>, (IpcErrorCode, String)> {
            let _gate = self.request_gate.lock().unwrap_or_else(|p| p.into_inner());
            if !self.has_capability(CapabilityKind::DatabaseExecute) {
                return Err((
                    IpcErrorCode::CapabilityDenied,
                    "host did not advertise the database capability".into(),
                ));
            }
            let parent_request_id = self.active_request_id.load(Ordering::Acquire);
            if parent_request_id == 0 {
                return Err((
                    IpcErrorCode::CapabilityDenied,
                    "database capability is unavailable before an invocation is active".into(),
                ));
            }

            let cancelled = unsafe { (cancellation.is_cancelled)(cancellation.context) };
            if cancelled {
                return Err((
                    IpcErrorCode::Cancelled,
                    "database capability was cancelled before dispatch".into(),
                ));
            }
            if payload.len() > MAX_DATABASE_CAPABILITY_BYTES {
                return Err((
                    IpcErrorCode::ResourceLimitExceeded,
                    format!(
                        "database capability request exceeded {MAX_DATABASE_CAPABILITY_BYTES} bytes"
                    ),
                ));
            }

            let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
            let operation_id = self.active_operation_id.load(Ordering::Acquire);
            let limits = *self.limits.lock().unwrap_or_else(|p| p.into_inner());
            let mut frame = IpcFrame::with_control(
                MessageType::CapabilityRequest,
                request_id,
                &(CapabilityRequestControl {
                    operation_id,
                    capability: CapabilityKind::DatabaseExecute,
                })
            ).map_err(|error| (
                IpcErrorCode::ProtocolViolation,
                format!("could not encode CapabilityRequest: {error}"),
            ))?;
            frame.correlation_id = Some(parent_request_id);
            frame.payload = payload.to_vec();
            self
                .write_frame(&frame, limits)
                .map_err(|error| (
                    IpcErrorCode::WorkerExited,
                    format!("could not send CapabilityRequest: {error}"),
                ))?;

            loop {
                let incoming = (
                    {
                        let mut reader = self.reader.lock().unwrap_or_else(|p| p.into_inner());
                        IpcFrame::read_from(&mut *reader, limits)
                    }
                ).map_err(|error| (
                    IpcErrorCode::WorkerExited,
                    format!("could not receive CapabilityResult: {error}"),
                ))?;

                if
                    incoming.message_type == MessageType::Cancel &&
                    incoming.request_id == parent_request_id
                {
                    if
                        let Some(cancellation) = self.active_cancellation
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .as_ref()
                            .and_then(Weak::upgrade)
                    {
                        cancellation.store(true, Ordering::Release);
                    }
                    continue;
                }
                if
                    incoming.message_type != MessageType::CapabilityResult ||
                    incoming.request_id != request_id ||
                    incoming.correlation_id != Some(parent_request_id)
                {
                    return Err((
                        IpcErrorCode::ProtocolViolation,
                        format!(
                            "expected CapabilityResult request_id={} correlation_id={}, received {:?} request_id={} correlation_id={:?}",
                            request_id,
                            parent_request_id,
                            incoming.message_type,
                            incoming.request_id,
                            incoming.correlation_id
                        ),
                    ));
                }
                let result: CapabilityResultControl = incoming
                    .decode_control()
                    .map_err(|error| {
                        (
                            IpcErrorCode::ProtocolViolation,
                            format!("invalid CapabilityResult control: {error}"),
                        )
                    })?;
                if result.success {
                    return Ok(incoming.payload);
                }
                return Err((
                    result.error_code.unwrap_or(IpcErrorCode::CapabilityDenied),
                    result.error_message.unwrap_or_else(||
                        "host denied database capability".into()
                    ),
                ));
            }
        }

        fn send_log(&self, level: LogLevel, message: String) {
            if !self.has_capability(CapabilityKind::Log) {
                eprintln!(
                    "ezf-plugin-worker: host did not advertise logging capability; plugin log suppressed"
                );
                return;
            }
            let mut message = message;
            if message.len() > MAX_PLUGIN_LOG_BYTES {
                let mut end = MAX_PLUGIN_LOG_BYTES;
                while !message.is_char_boundary(end) {
                    end -= 1;
                }
                message.truncate(end);
                message.push_str("… [truncated]");
            }
            let limits = *self.limits.lock().unwrap_or_else(|p| p.into_inner());
            match IpcFrame::with_control(MessageType::Log, 0, &(LogControl { level, message })) {
                Ok(frame) => {
                    if let Err(error) = self.write_frame(&frame, limits) {
                        eprintln!("ezf-plugin-worker: failed to send plugin log over IPC: {error}");
                    }
                }
                Err(error) => eprintln!("ezf-plugin-worker: failed to encode plugin log: {error}"),
            }
        }
    }

    struct ActiveCallGuard {
        channel: &'static WorkerCapabilityChannel,
        request_id: u64,
    }

    impl Drop for ActiveCallGuard {
        fn drop(&mut self) {
            self.channel.end_call(self.request_id);
        }
    }

    pub fn main_entry() -> Result<(), String> {
        let (socket_path, allow_unconfined) = parse_args()?;
        if !allow_unconfined {
            return Err(
                "this integration worker only runs when --allow-unconfined is explicit; no OS sandbox is implemented".into()
            );
        }
        let mut stream = UnixStream::connect(&socket_path).map_err(|error|
            format!("could not connect to private IPC socket {}: {error}", socket_path.display())
        )?;
        stream.set_read_timeout(None).map_err(|e| format!("could not configure IPC socket: {e}"))?;
        stream.set_write_timeout(None).map_err(|e| format!("could not configure IPC socket: {e}"))?;
        let writer = stream.try_clone().map_err(|e| format!("could not clone IPC writer: {e}"))?;
        let reader = stream.try_clone().map_err(|e| format!("could not clone IPC reader: {e}"))?;
        WORKER_CHANNEL.set(WorkerCapabilityChannel::new(writer, reader)).map_err(|_|
            "worker IPC channel was initialized more than once".to_owned()
        )?;
        serve(&mut stream, allow_unconfined)
    }

    fn parse_args() -> Result<(PathBuf, bool), String> {
        let mut args = std::env::args_os().skip(1);
        let mut socket = None;
        let mut allow_unconfined = false;
        while let Some(arg) = args.next() {
            if arg == "--ipc-socket" {
                let value = args.next().ok_or("--ipc-socket requires a path")?;
                socket = Some(PathBuf::from(value));
            } else if arg == "--allow-unconfined" {
                allow_unconfined = true;
            } else if arg == "--help" || arg == "-h" {
                println!("ezf-plugin-worker --ipc-socket <absolute-path> --allow-unconfined");
                std::process::exit(0);
            } else {
                return Err(format!("unknown argument: {}", arg.to_string_lossy()));
            }
        }
        let socket = socket.ok_or("missing required --ipc-socket <path>")?;
        if !socket.is_absolute() {
            return Err("IPC socket path must be absolute".into());
        }
        Ok((socket, allow_unconfined))
    }

    fn serve(stream: &mut UnixStream, allow_unconfined: bool) -> Result<(), String> {
        let mut limits = FrameLimits::default();
        let mut core: Option<PluginExecutionCore::PluginExecutionCore> = None;
        let mut initialized: Option<InitializeControl> = None;
        let mut on_unload_completed = false;
        let mut destroyed = false;
        let started_at = Instant::now();

        let result = (|| {
            // No library is loaded until the host explicitly selects the development-only
            // unconfined profile and sends LoadPlugin.
            let hello = read_frame(stream, limits)?;
            if hello.message_type != MessageType::Hello {
                send_protocol_error(
                    stream,
                    hello.request_id,
                    IpcErrorCode::UnsupportedProtocol,
                    "expected Hello as the first supervisor message",
                    limits
                )?;
                return Err("expected Hello as the first supervisor message".into());
            }
            let hello_control: HelloControl = decode_control(&hello)?;
            if
                hello_control.protocol_major != IPC_MAJOR_VERSION ||
                hello_control.protocol_minor != IPC_MINOR_VERSION
            {
                send_protocol_error(
                    stream,
                    hello.request_id,
                    IpcErrorCode::UnsupportedProtocol,
                    "host and worker IPC versions do not match",
                    limits
                )?;
                return Err("host and worker IPC versions do not match".into());
            }
            let response = HelloControl {
                protocol_major: IPC_MAJOR_VERSION,
                protocol_minor: IPC_MINOR_VERSION,
                worker_build: format!("ezf-plugin-worker/{WORKER_BUILD}"),
            };
            send_control(
                stream,
                MessageType::Hello,
                hello.request_id,
                &response,
                Vec::new(),
                limits
            )?;

            let init_frame = read_frame(stream, limits)?;
            if init_frame.message_type != MessageType::Initialize {
                send_protocol_error(
                    stream,
                    init_frame.request_id,
                    IpcErrorCode::ProtocolViolation,
                    "expected Initialize after Hello",
                    limits
                )?;
                return Err("expected Initialize after Hello".into());
            }
            let init: InitializeControl = decode_control(&init_frame)?;
            if init.protocol_major != IPC_MAJOR_VERSION || init.protocol_minor != IPC_MINOR_VERSION {
                send_protocol_error(
                    stream,
                    init_frame.request_id,
                    IpcErrorCode::UnsupportedProtocol,
                    "unsupported IPC version in Initialize",
                    limits
                )?;
                return Err("unsupported IPC version in Initialize".into());
            }
            if
                !allow_unconfined ||
                init.sandbox_profile != PluginIpc::SandboxProfile::DevelopmentUnconfined
            {
                send_protocol_error(
                    stream,
                    init_frame.request_id,
                    IpcErrorCode::SandboxSetupFailed,
                    "this worker build only supports explicit DevelopmentUnconfined integration mode",
                    limits
                )?;
                return Err("this worker build only supports DevelopmentUnconfined".into());
            }
            if init.runtime_limits.max_concurrent_calls == 0 {
                send_protocol_error(
                    stream,
                    init_frame.request_id,
                    IpcErrorCode::ResourceLimitExceeded,
                    "max_concurrent_calls must be at least one",
                    limits
                )?;
                return Err("max_concurrent_calls must be at least one".into());
            }
            // The host may tighten the default protocol limit, but cannot increase it through
            // configuration. Subsequent frames are checked against this negotiated maximum.
            limits.max_payload_bytes = (init.runtime_limits.max_payload_bytes as usize).min(
                DEFAULT_MAX_PAYLOAD_BYTES
            );
            limits.max_frame_bytes = IPC_HEADER_LEN.saturating_add(
                DEFAULT_MAX_CONTROL_BYTES
            ).saturating_add(limits.max_payload_bytes);
            if let Some(channel) = WORKER_CHANNEL.get() {
                channel.set_limits(limits);
                channel.set_capabilities(init.allowed_capabilities.clone());
            }
            let sandbox = SandboxReadyControl {
                profile: PluginIpc::SandboxProfile::DevelopmentUnconfined,
                sandbox_enforced: false,
                active_features: vec!["ipc_capability_broker".to_owned(), "ipc_logging".to_owned()],
                missing_features: vec!["os_sandbox_not_implemented".to_owned()],
            };
            send_control(
                stream,
                MessageType::SandboxReady,
                init_frame.request_id,
                &sandbox,
                Vec::new(),
                limits
            )?;
            initialized = Some(init);

            let load_frame = read_frame(stream, limits)?;
            if load_frame.message_type != MessageType::LoadPlugin {
                send_protocol_error(
                    stream,
                    load_frame.request_id,
                    IpcErrorCode::ProtocolViolation,
                    "expected LoadPlugin after SandboxReady",
                    limits
                )?;
                return Err("expected LoadPlugin after SandboxReady".into());
            }
            let load: LoadPluginControl = decode_control(&load_frame)?;
            if load.expected_abi_version != PLUGIN_ABI_VERSION_V2 {
                send_protocol_error(
                    stream,
                    load_frame.request_id,
                    IpcErrorCode::UnsupportedProtocol,
                    "requested plugin ABI version is not supported",
                    limits
                )?;
                return Err("requested plugin ABI version is not supported".into());
            }
            let init = initialized.as_ref().ok_or("worker initialization state is missing")?;
            let plugin_path = PathBuf::from(&init.plugin_path);
            if !plugin_path.is_absolute() || !plugin_path.is_file() {
                send_protocol_error(
                    stream,
                    load_frame.request_id,
                    IpcErrorCode::PluginError,
                    "plugin_path must be an absolute path to an existing library",
                    limits
                )?;
                return Err("plugin_path must be an absolute path to an existing library".into());
            }
            let plugin_directory = if init.plugin_directory.is_empty() {
                plugin_path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .to_path_buf()
            } else {
                PathBuf::from(&init.plugin_directory)
            };
            if !plugin_directory.is_absolute() {
                return Err("plugin_directory must be an absolute path".into());
            }
            let directory_bytes = plugin_directory.to_string_lossy().into_owned().into_bytes();
            let directory_slice = AbiSlice::from_bytes(&directory_bytes);
            // ABI callbacks stay process-local. Database requests and plugin logs are brokered
            // over the private IPC channel; the supervisor applies per-plugin policy before DB IO.
            let host_services = PluginHostServicesV2::new(
                worker_log,
                Some(worker_database_execute),
                Some(worker_free_buffer),
                directory_slice
            );
            let loaded = PluginExecutionCore::PluginExecutionCore::load(
                &plugin_path,
                &host_services
            ).map_err(|error| {
                match error {
                    PluginLoadError::Library(error) =>
                        format!("failed to load plugin library: {error}"),
                    PluginLoadError::Constructor(error) =>
                        format!("failed to load InstantiateV2: {error}"),
                }
            });
            let loaded = match loaded {
                Ok(core) => core,
                Err(message) => {
                    send_protocol_error(
                        stream,
                        load_frame.request_id,
                        IpcErrorCode::PluginError,
                        &message,
                        limits
                    )?;
                    return Err(message);
                }
            };
            if let Err(message) = loaded.validate() {
                // Validation failed, so callback pointers may be invalid. Do not call lifecycle
                // functions through an ABI layout that has just failed validation, and do not
                // dlclose the potentially malformed library while unwinding this scope.
                send_protocol_error(
                    stream,
                    load_frame.request_id,
                    IpcErrorCode::ProtocolViolation,
                    &message,
                    limits
                )?;
                std::process::exit(3);
            }
            let no_cancel = AtomicBool::new(false);
            let cancellation = cancellation_for(&no_cancel);
            let name_bytes = match loaded.name_bytes(cancellation) {
                Ok(bytes) => bytes,
                Err(message) => {
                    loaded.emergency_shutdown();
                    send_protocol_error(
                        stream,
                        load_frame.request_id,
                        IpcErrorCode::PluginError,
                        &message,
                        limits
                    )?;
                    return Err(message);
                }
            };
            let plugin_name: String = match serde_json::from_slice(&name_bytes) {
                Ok(name) => name,
                Err(error) => {
                    loaded.emergency_shutdown();
                    let message = format!("plugin name was not a JSON string: {error}");
                    send_protocol_error(
                        stream,
                        load_frame.request_id,
                        IpcErrorCode::PluginError,
                        &message,
                        limits
                    )?;
                    return Err(message);
                }
            };
            let manifest_bytes = match loaded.manifest_bytes(cancellation) {
                Ok(bytes) => bytes,
                Err(message) => {
                    loaded.emergency_shutdown();
                    send_protocol_error(
                        stream,
                        load_frame.request_id,
                        IpcErrorCode::PluginError,
                        &message,
                        limits
                    )?;
                    return Err(message);
                }
            };
            let manifest_json = match String::from_utf8(manifest_bytes) {
                Ok(value) => value,
                Err(error) => {
                    loaded.emergency_shutdown();
                    let message = format!("plugin manifest is not UTF-8: {error}");
                    send_protocol_error(
                        stream,
                        load_frame.request_id,
                        IpcErrorCode::PluginError,
                        &message,
                        limits
                    )?;
                    return Err(message);
                }
            };
            let manifest_value: serde_json::Value = match serde_json::from_str(&manifest_json) {
                Ok(value) => value,
                Err(error) => {
                    loaded.emergency_shutdown();
                    let message = format!("plugin manifest is not valid JSON: {error}");
                    send_protocol_error(
                        stream,
                        load_frame.request_id,
                        IpcErrorCode::PluginError,
                        &message,
                        limits
                    )?;
                    return Err(message);
                }
            };
            let manifest_name = manifest_value.get("name").and_then(|value| value.as_str());
            if manifest_name != Some(plugin_name.as_str()) {
                loaded.emergency_shutdown();
                let message = "plugin name and manifest name do not match";
                send_protocol_error(
                    stream,
                    load_frame.request_id,
                    IpcErrorCode::PluginError,
                    message,
                    limits
                )?;
                return Err(message.into());
            }
            let response = InitializedControl {
                plugin_name,
                manifest_json,
                abi_version: PLUGIN_ABI_VERSION_V2,
            };
            core = Some(loaded);
            send_control(
                stream,
                MessageType::Initialized,
                load_frame.request_id,
                &response,
                Vec::new(),
                limits
            )?;

            loop {
                let frame = read_frame(stream, limits)?;
                match frame.message_type {
                    MessageType::Invoke => {
                        let control: InvokeControl = decode_control(&frame)?;
                        let destroys_instance = control.operation == InvokeOperation::Destroy;
                        let calls_unload = control.operation == InvokeOperation::OnUnload;
                        let result = invoke(
                            core.as_ref(),
                            frame.request_id,
                            control,
                            &frame.payload
                        );
                        if calls_unload && result.is_ok() {
                            on_unload_completed = true;
                        }
                        if destroys_instance && result.is_ok() {
                            // destroy() may free the ABI instance. Drop the core only after the
                            // callback returns and never call into that instance again.
                            core = None;
                            destroyed = true;
                        }
                        match result {
                            Ok(payload) =>
                                send_control(
                                    stream,
                                    MessageType::Result,
                                    frame.request_id,
                                    &(ResultControl {
                                        success: true,
                                        error_code: None,
                                        error_message: None,
                                    }),
                                    payload,
                                    limits
                                )?,
                            Err((code, message, payload)) =>
                                send_control(
                                    stream,
                                    MessageType::Result,
                                    frame.request_id,
                                    &(ResultControl {
                                        success: false,
                                        error_code: Some(code),
                                        error_message: Some(message),
                                    }),
                                    payload,
                                    limits
                                )?,
                        }
                    }
                    MessageType::HealthProbe => {
                        let probe: PluginIpc::HealthProbeControl = decode_control(&frame)?;
                        send_control(
                            stream,
                            MessageType::Pong,
                            frame.request_id,
                            &(PongControl {
                                probe_id: probe.probe_id,
                                worker_uptime_ms: started_at.elapsed().as_millis() as u64,
                            }),
                            Vec::new(),
                            limits
                        )?;
                    }
                    MessageType::Cancel => {
                        // While an ABI callback is waiting for a brokered capability, its cloned
                        // socket reader consumes the matching Cancel and flips the active call's
                        // cancellation flag. Reaching this branch therefore means the Cancel is
                        // stale/idle (for example, the call already returned before it arrived).
                        eprintln!(
                            "ezf-plugin-worker: ignored stale/idle Cancel request_id={}",
                            frame.request_id
                        );
                    }
                    MessageType::Shutdown => {
                        let cleanup = if let Some(loaded) = core.as_ref() {
                            let cancellation = never_cancelled_descriptor();
                            let unload_result = if on_unload_completed {
                                Ok(())
                            } else {
                                match loaded.on_unload(cancellation) {
                                    Ok(PLUGIN_STATUS_OK) => {
                                        on_unload_completed = true;
                                        Ok(())
                                    }
                                    Ok(status) =>
                                        Err(
                                            format!("plugin on_unload failed with status {status}")
                                        ),
                                    Err(error) => Err(format!("plugin on_unload failed: {error}")),
                                }
                            };
                            match unload_result {
                                Ok(()) =>
                                    match loaded.destroy(cancellation) {
                                        Ok(PLUGIN_STATUS_OK) => {
                                            destroyed = true;
                                            Ok(())
                                        }
                                        Ok(status) =>
                                            Err(
                                                format!(
                                                    "plugin destroy failed with status {status}"
                                                )
                                            ),
                                        Err(error) =>
                                            Err(format!("plugin destroy failed: {error}")),
                                    }
                                Err(error) => Err(error),
                            }
                        } else if destroyed && on_unload_completed {
                            Ok(())
                        } else if destroyed {
                            Err(
                                "plugin instance was destroyed without a successful on_unload".into()
                            )
                        } else {
                            Err("plugin is not loaded".into())
                        };
                        let graceful = cleanup.is_ok();
                        let message = cleanup.err();
                        send_control(
                            stream,
                            MessageType::ShutdownComplete,
                            frame.request_id,
                            &(ShutdownCompleteControl { graceful, message }),
                            Vec::new(),
                            limits
                        )?;
                        if graceful {
                            // destroy() may have invalidated the opaque ABI instance; only now
                            // drop the wrapper and its library after both callbacks succeeded.
                            core = None;
                            return Ok(());
                        }
                        // Do not dlclose a library whose lifecycle callback failed. Exit the child
                        // directly so OS process teardown reclaims all plugin resources together.
                        std::process::exit(2);
                    }
                    MessageType::CapabilityResult => {
                        return Err(
                            "unexpected CapabilityResult: no capability request is pending".into()
                        );
                    }
                    _ => {
                        send_protocol_error(
                            stream,
                            frame.request_id,
                            IpcErrorCode::ProtocolViolation,
                            "message type is not valid in the worker request loop",
                            limits
                        )?;
                        return Err(format!("unexpected message type {:?}", frame.message_type));
                    }
                }
            }
        })();

        if result.is_err() {
            if let Some(loaded) = core.as_ref() {
                loaded.emergency_shutdown();
            }
        }
        result
    }

    fn invoke(
        core: Option<&PluginExecutionCore::PluginExecutionCore>,
        request_id: u64,
        control: InvokeControl,
        payload: &[u8]
    ) -> Result<Vec<u8>, (IpcErrorCode, String, Vec<u8>)> {
        if control.remaining_budget_ms == 0 {
            return Err((
                IpcErrorCode::Cancelled,
                "request budget expired before execution".into(),
                Vec::new(),
            ));
        }
        let core = core.ok_or_else(|| (
            IpcErrorCode::WorkerExited,
            "plugin is not loaded".into(),
            Vec::new(),
        ))?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancellation = cancellation_for(&cancelled);
        let _active_call = WORKER_CHANNEL.get().map(|channel| {
            channel.begin_call(request_id, control.operation_id, &cancelled);
            ActiveCallGuard { channel, request_id }
        });
        match control.operation {
            InvokeOperation::Name => core.name_bytes(cancellation).map_err(plugin_error),
            InvokeOperation::Manifest => core.manifest_bytes(cancellation).map_err(plugin_error),
            InvokeOperation::IsLoaded =>
                core
                    .is_loaded(cancellation)
                    .map(|loaded| serde_json::to_vec(&loaded).unwrap_or_else(|_| b"false".to_vec()))
                    .map_err(plugin_error),
            InvokeOperation::HandleEndpoint =>
                match core.handle_endpoint(payload, cancellation).map_err(plugin_error)? {
                    EndpointCallOutput::Serialized(bytes) => Ok(bytes),
                    EndpointCallOutput::Raw { metadata, body } =>
                        reassemble_raw_endpoint(metadata, body).map_err(plugin_error),
                }
            InvokeOperation::ExecuteDatabase =>
                core.execute_database_bytes(payload, cancellation).map_err(plugin_error),
            InvokeOperation::SessionClosed =>
                core.on_session_closed_bytes(payload, cancellation).map_err(plugin_error),
            InvokeOperation::OnUnload =>
                core
                    .on_unload(cancellation)
                    .map_err(plugin_error)
                    .and_then(|status| {
                        if status == PLUGIN_STATUS_OK {
                            Ok(serde_json::to_vec(&status).unwrap_or_default())
                        } else {
                            Err((
                                IpcErrorCode::PluginError,
                                format!("plugin on_unload failed with status {status}"),
                                serde_json::to_vec(&status).unwrap_or_default(),
                            ))
                        }
                    }),
            InvokeOperation::Destroy => {
                let status = core.destroy(cancellation).map_err(plugin_error)?;
                if status == PLUGIN_STATUS_OK {
                    Ok(serde_json::to_vec(&status).unwrap_or_default())
                } else {
                    Err((
                        IpcErrorCode::PluginError,
                        format!("plugin destroy failed with status {status}"),
                        serde_json::to_vec(&status).unwrap_or_default(),
                    ))
                }
            }
        }
    }

    fn reassemble_raw_endpoint(metadata: Vec<u8>, body: Vec<u8>) -> Result<Vec<u8>, String> {
        let mut value: serde_json::Value = serde_json
            ::from_slice(&metadata)
            .map_err(|error| format!("invalid raw endpoint metadata JSON: {error}"))?;
        let object = value.as_object_mut().ok_or("raw endpoint metadata must be a JSON object")?;
        if let Some(success) = object.get_mut("Ok") {
            let response = success
                .as_object_mut()
                .ok_or("raw endpoint Ok value must be an object")?;
            let current_body = response
                .get("body")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            if !current_body.is_empty() {
                return Err(
                    "raw endpoint returned a body in both metadata and binary buffer".into()
                );
            }
            let body_value = match std::str::from_utf8(&body) {
                Ok(text) => serde_json::Value::String(text.to_owned()),
                Err(_) =>
                    serde_json::Value::Array(
                        body.iter().copied().map(serde_json::Value::from).collect()
                    ),
            };
            response.insert("body".to_owned(), body_value);
        } else if !body.is_empty() {
            return Err("raw endpoint returned a binary body alongside an error".into());
        }
        serde_json
            ::to_vec(&value)
            .map_err(|error| format!("could not encode raw endpoint response: {error}"))
    }

    fn plugin_error(message: String) -> (IpcErrorCode, String, Vec<u8>) {
        (IpcErrorCode::PluginError, message, Vec::new())
    }

    fn cancellation_for(cancelled: &AtomicBool) -> PluginCancellationV2 {
        PluginCancellationV2 {
            context: (cancelled as *const AtomicBool).cast(),
            is_cancelled: is_cancelled,
        }
    }

    fn never_cancelled_descriptor() -> PluginCancellationV2 {
        // `context` is only inspected during this synchronous call. The pointer must remain valid;
        // the caller uses this helper only for immediate lifecycle callback invocation.
        static NEVER_CANCELLED: AtomicBool = AtomicBool::new(false);
        cancellation_for(&NEVER_CANCELLED)
    }

    unsafe extern "C" fn is_cancelled(context: *const c_void) -> bool {
        if context.is_null() {
            return true;
        }
        let flag = unsafe { &*(context as *const AtomicBool) };
        flag.load(Ordering::Acquire)
    }

    unsafe extern "C" fn worker_log(level: u8, message: AbiSlice) {
        let text = (unsafe { message.to_vec() })
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_else(|| "<plugin supplied invalid log buffer>".to_owned());
        let level = match level {
            1 => LogLevel::Error,
            2 => LogLevel::Warn,
            3 => LogLevel::Info,
            4 => LogLevel::Debug,
            5 => LogLevel::Trace,
            _ => LogLevel::Error,
        };
        if let Some(channel) = WORKER_CHANNEL.get() {
            channel.send_log(level, text);
        } else {
            eprintln!("ezf-plugin-worker plugin-log[{level:?}]: {text}");
        }
    }

    unsafe extern "C" fn worker_database_execute(
        request: AbiSlice,
        cancellation: PluginCancellationV2,
        response_out: *mut AbiBuffer
    ) -> i32 {
        if response_out.is_null() {
            return PLUGIN_STATUS_INVALID_ARGUMENT;
        }
        unsafe {
            *response_out = AbiBuffer::EMPTY;
        }

        let vc = unsafe { request.to_vec() };
        let Some(request_bytes) = vc else {
            return PLUGIN_STATUS_INVALID_ARGUMENT;
        };
        let Some(channel) = WORKER_CHANNEL.get() else {
            let result = database_error_envelope(
                DatabaseErrorKind::Unsupported,
                "worker IPC capability channel is unavailable"
            );
            unsafe {
                *response_out = AbiBuffer::from_vec(result);
            }
            return PLUGIN_STATUS_OK;
        };
        let result = match channel.request_database(&request_bytes, cancellation) {
            Ok(response) => response,
            Err((code, message)) => database_error_envelope(database_error_kind(code), &message),
        };
        unsafe {
            *response_out = AbiBuffer::from_vec(result);
        }
        PLUGIN_STATUS_OK
    }

    unsafe extern "C" fn worker_free_buffer(buffer: AbiBuffer) {
        unsafe {
            buffer.free();
        }
    }

    fn database_error_kind(code: IpcErrorCode) -> DatabaseErrorKind {
        match code {
            IpcErrorCode::CapabilityDenied => DatabaseErrorKind::Unsupported,
            IpcErrorCode::ResourceLimitExceeded | IpcErrorCode::ProtocolViolation =>
                DatabaseErrorKind::InvalidRequest,
            IpcErrorCode::Cancelled => DatabaseErrorKind::Query,
            | IpcErrorCode::WorkerExited
            | IpcErrorCode::SandboxSetupFailed
            | IpcErrorCode::UnsupportedProtocol
            | IpcErrorCode::PluginError => DatabaseErrorKind::Connection,
        }
    }

    fn database_error_envelope(kind: DatabaseErrorKind, message: &str) -> Vec<u8> {
        serde_json
            ::to_vec(
                &Result::<DatabaseResponse, DatabaseError>::Err(DatabaseError::new(kind, message))
            )
            .unwrap_or_else(|_|
                b"{\"Err\":{\"kind\":\"unsupported\",\"message\":\"database capability failed\"}}".to_vec()
            )
    }

    fn read_frame(stream: &mut UnixStream, limits: FrameLimits) -> Result<IpcFrame, String> {
        IpcFrame::read_from(stream, limits).map_err(|error| error.to_string())
    }

    fn decode_control<T: serde::de::DeserializeOwned>(frame: &IpcFrame) -> Result<T, String> {
        frame.decode_control().map_err(|error| error.to_string())
    }

    fn send_control<T: serde::Serialize>(
        stream: &mut UnixStream,
        message_type: MessageType,
        request_id: u64,
        control: &T,
        payload: Vec<u8>,
        limits: FrameLimits
    ) -> Result<(), String> {
        let mut frame = IpcFrame::with_control(message_type, request_id, control).map_err(|error|
            error.to_string()
        )?;
        frame.payload = payload;
        if let Some(channel) = WORKER_CHANNEL.get() {
            channel.write_frame(&frame, limits)
        } else {
            frame.write_to(stream, limits).map_err(|error| error.to_string())
        }
    }

    fn send_protocol_error(
        stream: &mut UnixStream,
        request_id: u64,
        code: IpcErrorCode,
        message: &str,
        limits: FrameLimits
    ) -> Result<(), String> {
        send_control(
            stream,
            MessageType::ProtocolError,
            request_id,
            &(ProtocolErrorControl { code, message: message.to_owned() }),
            Vec::new(),
            limits
        )
    }
}

#[cfg(unix)]
fn main() {
    if let Err(error) = unix_worker::main_entry() {
        eprintln!("ezf-plugin-worker: {error}");
        std::process::exit(1);
    }
}

#[cfg(not(unix))]
fn main() {
    eprintln!(
        "ezf-plugin-worker currently supports Unix-domain sockets only; no Windows transport is implemented"
    );
    std::process::exit(2);
}
