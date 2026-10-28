#![cfg(unix)]

use PluginIpc::{
    FrameLimits,
    HealthProbeControl,
    HelloControl,
    InitializeControl,
    InitializedControl,
    InvokeControl,
    InvokeOperation,
    LoadPluginControl,
    MessageType,
    PongControl,
    ResultControl,
    RuntimeLimits,
    SandboxProfile,
    SandboxReadyControl,
    ShutdownCompleteControl,
    ShutdownControl,
    IpcFrame,
    IPC_MAJOR_VERSION,
    IPC_MINOR_VERSION,
};
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{ UnixListener, UnixStream };
use std::path::PathBuf;
use std::process::{ Child, Command, Stdio };
use std::sync::atomic::{ AtomicUsize, Ordering };
use std::time::Duration;

fn plugin_fixture_manifest() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../PluginHost/tests/pluginhost_integration_fixture/Cargo.toml"
    )
}

fn shared_library_name() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "libpluginhost_integration_fixture.so"
    }
    #[cfg(target_os = "macos")]
    {
        "libpluginhost_integration_fixture.dylib"
    }
    #[cfg(target_os = "windows")]
    {
        "pluginhost_integration_fixture.dll"
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        "libpluginhost_integration_fixture.so"
    }
}

fn build_real_plugin() -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let temp = std::env
        ::temp_dir()
        .join(
            format!(
                "ezf-plugin-worker-fixture-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            )
        );
    let target = temp.join("target");
    fs::create_dir_all(&target).expect("fixture target directory must be creatable");
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = Command::new(cargo)
        .arg("build")
        .arg("--manifest-path")
        .arg(plugin_fixture_manifest())
        .arg("--locked")
        .env_remove("EZF_PLUGIN_FIXTURE_MODE")
        .arg("--target-dir")
        .arg(&target)
        .arg("--quiet")
        .status()
        .expect("cargo must be available to build the real plugin fixture");
    assert!(status.success(), "the real plugin fixture must compile");
    target.join("debug").join(shared_library_name())
}

fn write_frame(stream: &mut UnixStream, frame: &IpcFrame) -> io::Result<()> {
    frame
        .write_to(stream, FrameLimits::default())
        .map_err(|error| io::Error::new(io::ErrorKind::Other, error.to_string()))
}

fn read_frame(stream: &mut UnixStream) -> IpcFrame {
    IpcFrame::read_from(stream, FrameLimits::default()).expect("worker frame must be readable")
}

fn send_control<T: serde::Serialize>(
    stream: &mut UnixStream,
    ty: MessageType,
    id: u64,
    control: &T,
    payload: Vec<u8>
) {
    let mut frame = IpcFrame::with_control(ty, id, control).expect("control must serialize");
    frame.payload = payload;
    write_frame(stream, &frame).expect("frame must be written");
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn assert_child_clean_exit(child: &mut Child) {
    let status = child.wait().expect("worker process must be reaped");
    assert!(status.success(), "worker must exit cleanly, got {status}");
}

#[test]
fn worker_refuses_unconfined_mode_unless_explicitly_enabled() {
    let status = Command::new(env!("CARGO_BIN_EXE_ezf-plugin-worker"))
        .arg("--ipc-socket")
        .arg(std::env::temp_dir().join("ezf-no-such-worker-socket.sock"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("worker binary must start");
    assert!(!status.success(), "worker must not silently start without --allow-unconfined");
}

#[test]
fn real_worker_process_loads_plugin_invokes_endpoint_and_shuts_down() -> io::Result<()> {
    let plugin = build_real_plugin();
    assert!(plugin.is_file(), "fixture shared library must exist: {}", plugin.display());

    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    // Keep the Unix-domain socket pathname short enough for macOS sockaddr_un limits.
    let socket_path = PathBuf::from("/tmp").join(
        format!("ezf-pw-{}-{}.sock", std::process::id(), COUNTER.fetch_add(1, Ordering::Relaxed))
    );
    let _ = fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path)?;
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;

    let child = Command::new(env!("CARGO_BIN_EXE_ezf-plugin-worker"))
        .arg("--ipc-socket")
        .arg(&socket_path)
        .arg("--allow-unconfined")
        .env_remove("EZF_PLUGIN_FIXTURE_MODE")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()?;
    let mut child = ChildGuard(child);
    let (mut stream, _) = listener.accept()?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;

    // Hello negotiation.
    send_control(
        &mut stream,
        MessageType::Hello,
        1,
        &(HelloControl {
            protocol_major: IPC_MAJOR_VERSION,
            protocol_minor: IPC_MINOR_VERSION,
            worker_build: "phase4-test-supervisor".into(),
        }),
        Vec::new()
    );
    let hello = read_frame(&mut stream);
    assert_eq!(hello.message_type, MessageType::Hello);
    let hello: HelloControl = hello.decode_control().expect("Hello response must decode");
    assert_eq!(hello.protocol_major, IPC_MAJOR_VERSION);
    assert!(hello.worker_build.starts_with("ezf-plugin-worker/"));

    // Explicitly select the development-only unconfined mode. No sandbox is claimed.
    send_control(
        &mut stream,
        MessageType::Initialize,
        2,
        &(InitializeControl {
            protocol_major: IPC_MAJOR_VERSION,
            protocol_minor: IPC_MINOR_VERSION,
            worker_build: "phase4-test-supervisor".into(),
            plugin_path: plugin.to_string_lossy().into_owned(),
            plugin_directory: plugin
                .parent()
                .expect("plugin has parent")
                .to_string_lossy()
                .into_owned(),
            allowed_capabilities: vec![],
            runtime_limits: RuntimeLimits {
                max_concurrent_calls: 1,
                max_payload_bytes: 1024 * 1024,
                cancellation_grace_ms: 100,
            },
            sandbox_profile: SandboxProfile::DevelopmentUnconfined,
        }),
        Vec::new()
    );
    let sandbox = read_frame(&mut stream);
    assert_eq!(sandbox.message_type, MessageType::SandboxReady);
    let sandbox: SandboxReadyControl = sandbox
        .decode_control()
        .expect("sandbox response must decode");
    assert_eq!(sandbox.profile, SandboxProfile::DevelopmentUnconfined);
    assert!(!sandbox.sandbox_enforced);
    assert!(sandbox.missing_features.iter().any(|feature| feature.contains("not_implemented")));

    // Load a separately compiled real cdylib inside the child process.
    send_control(
        &mut stream,
        MessageType::LoadPlugin,
        3,
        &(LoadPluginControl { expected_abi_version: 2 }),
        Vec::new()
    );
    let initialized = read_frame(&mut stream);
    assert_eq!(initialized.message_type, MessageType::Initialized);
    let initialized: InitializedControl = initialized
        .decode_control()
        .expect("Initialized must decode");
    assert_eq!(initialized.plugin_name, "PluginHostIntegration");
    assert_eq!(initialized.abi_version, 2);
    let manifest: serde_json::Value = serde_json
        ::from_str(&initialized.manifest_json)
        .expect("manifest is JSON");
    assert_eq!(manifest["name"], "PluginHostIntegration");

    // Exercise an actual ABI callback across the process boundary.
    send_control(
        &mut stream,
        MessageType::Invoke,
        77,
        &(InvokeControl {
            operation: InvokeOperation::HandleEndpoint,
            remaining_budget_ms: 5_000,
            operation_id: 0,
        }),
        serde_json
            ::to_vec(
                &serde_json::json!({
        "endpoint_id": "ping",
        "method": "Get",
        "raw_path": "/pluginhost-integration/ping",
        "base_path": "/",
        "raw_query": null,
        "content_type": null,
        "body": "",
        "principal": {
            "user_id": "integration-test",
            "username": "integration-test",
            "given_name": "Integration",
            "family_name": "Test",
            "groups": [],
            "session_id": "integration-session"
        }
    })
            )
            .expect("endpoint request must serialize")
    );
    let result = read_frame(&mut stream);
    assert_eq!(result.message_type, MessageType::Result);
    assert_eq!(result.request_id, 77);
    let result_control: ResultControl = result.decode_control().expect("Result must decode");
    assert!(result_control.success, "plugin call failed: {:?}", result_control.error_message);
    let body: serde_json::Value = serde_json
        ::from_slice(&result.payload)
        .expect("endpoint payload must be JSON");
    assert_eq!(body["Ok"]["status"], 200);
    let endpoint_body: serde_json::Value = serde_json
        ::from_str(
            body["Ok"]["body"].as_str().expect("raw endpoint body must be reassembled as text")
        )
        .expect("reassembled endpoint body must be JSON");
    assert_eq!(endpoint_body["plugin"], "PluginHostIntegration");
    assert_eq!(endpoint_body["endpoint"], "ping");
    assert_eq!(endpoint_body["loaded"], true);

    // Health and graceful teardown are also driven over the real process channel.
    send_control(
        &mut stream,
        MessageType::HealthProbe,
        78,
        &(HealthProbeControl { probe_id: 901 }),
        Vec::new()
    );
    let pong = read_frame(&mut stream);
    assert_eq!(pong.message_type, MessageType::Pong);
    let pong: PongControl = pong.decode_control().expect("Pong must decode");
    assert_eq!(pong.probe_id, 901);

    send_control(
        &mut stream,
        MessageType::Shutdown,
        99,
        &(ShutdownControl { grace_budget_ms: 1_000 }),
        Vec::new()
    );
    let shutdown = read_frame(&mut stream);
    assert_eq!(shutdown.message_type, MessageType::ShutdownComplete);
    let shutdown: ShutdownCompleteControl = shutdown
        .decode_control()
        .expect("ShutdownComplete must decode");
    assert!(shutdown.graceful, "worker cleanup failed: {:?}", shutdown.message);

    assert_child_clean_exit(&mut child.0);
    drop(listener);
    let _ = fs::remove_file(&socket_path);
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn enforced_domain_sandbox_blocks_network_filesystem_and_process_creation_or_fails_closed() -> io::Result<()> {
    let plugin = build_real_plugin();
    assert!(plugin.is_file(), "fixture shared library must exist: {}", plugin.display());

    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let sandbox_dir = std::env
        ::temp_dir()
        .join(
            format!(
                "ezf-sandbox-it-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            )
        );
    fs::create_dir(&sandbox_dir)?;
    fs::set_permissions(&sandbox_dir, fs::Permissions::from_mode(0o700))?;
    let socket_path = sandbox_dir.join("worker.sock");
    let plugin_temp = sandbox_dir.join("plugin-tmp");
    fs::create_dir(&plugin_temp)?;
    fs::set_permissions(&plugin_temp, fs::Permissions::from_mode(0o700))?;
    let listener = UnixListener::bind(&socket_path)?;
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;

    let worker_binary = env!("CARGO_BIN_EXE_ezf-plugin-worker");
    let child = Command::new(worker_binary)
        .arg("--ipc-socket")
        .arg(&socket_path)
        .arg("--sandbox-profile")
        .arg("domain_plugin")
        .arg("--plugin-temp-dir")
        .arg(&plugin_temp)
        .env_clear()
        .env("TMPDIR", &plugin_temp)
        .env("HOME", &plugin_temp)
        .current_dir(plugin.parent().expect("fixture plugin has parent"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()?;
    let mut child = ChildGuard(child);
    let (mut stream, _) = listener.accept()?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;

    send_control(
        &mut stream,
        MessageType::Hello,
        1,
        &(HelloControl {
            protocol_major: IPC_MAJOR_VERSION,
            protocol_minor: IPC_MINOR_VERSION,
            worker_build: "phase8-sandbox-test".into(),
        }),
        Vec::new()
    );
    let hello = read_frame(&mut stream);
    assert_eq!(hello.message_type, MessageType::Hello);

    send_control(
        &mut stream,
        MessageType::Initialize,
        2,
        &(InitializeControl {
            protocol_major: IPC_MAJOR_VERSION,
            protocol_minor: IPC_MINOR_VERSION,
            worker_build: "phase8-sandbox-test".into(),
            plugin_path: plugin.to_string_lossy().into_owned(),
            plugin_directory: plugin
                .parent()
                .expect("fixture plugin has parent")
                .to_string_lossy()
                .into_owned(),
            allowed_capabilities: vec![PluginIpc::CapabilityKind::Log],
            runtime_limits: RuntimeLimits {
                max_concurrent_calls: 1,
                max_payload_bytes: 1024 * 1024,
                cancellation_grace_ms: 100,
            },
            sandbox_profile: SandboxProfile::DomainPlugin,
        }),
        Vec::new()
    );
    let sandbox_frame = read_frame(&mut stream);
    if sandbox_frame.message_type == MessageType::ProtocolError {
        let error: ProtocolErrorControl = sandbox_frame
            .decode_control()
            .expect("ProtocolError must decode");
        assert_eq!(error.code, PluginIpc::IpcErrorCode::SandboxSetupFailed);
        if
            std::env
                ::var("EZF_REQUIRE_ACTIVE_SANDBOX_TEST")
                .map(|value| value == "1")
                .unwrap_or(false)
        {
            panic!(
                "EZF_REQUIRE_ACTIVE_SANDBOX_TEST=1, but enforced sandbox setup failed closed: {}",
                error.message
            );
        }
        assert!(
            !error.message.trim().is_empty(),
            "fail-closed diagnostic should identify why sandbox setup was refused"
        );
        let status = child.0.wait().expect("sandbox setup failure must reap the worker");
        assert!(!status.success(), "worker must not continue unconfined when enforcement fails");
        drop(listener);
        let _ = fs::remove_dir_all(&sandbox_dir);
        return Ok(());
    }
    assert_eq!(sandbox_frame.message_type, MessageType::SandboxReady);
    let sandbox: SandboxReadyControl = sandbox_frame
        .decode_control()
        .expect("SandboxReady must decode");
    assert_eq!(sandbox.profile, SandboxProfile::DomainPlugin);
    assert!(
        sandbox.sandbox_enforced,
        "worker must not report an enforced profile without applying it"
    );
    for required in [
        "prctl_no_new_privs",
        "seccomp_direct_network_syscalls_denied",
        "seccomp_process_creation_and_privilege_syscalls_restricted",
        "seccomp_cross_process_signals_and_capability_mutation_denied",
        "effective_capabilities_zero",
        "inherited_file_descriptors_closed",
        "landlock_private_writable_temp_directory",
    ] {
        assert!(
            sandbox.active_features.iter().any(|feature| feature == required),
            "missing sandbox feature {required}: {:?}",
            sandbox.active_features
        );
    }

    send_control(
        &mut stream,
        MessageType::LoadPlugin,
        3,
        &(LoadPluginControl { expected_abi_version: 2 }),
        Vec::new()
    );
    let initialized = read_frame(&mut stream);
    assert_eq!(
        initialized.message_type,
        MessageType::Initialized,
        "enforced sandbox must be active before loading the plugin"
    );
    let initialized: InitializedControl = initialized
        .decode_control()
        .expect("Initialized must decode");
    assert_eq!(initialized.plugin_name, "PluginHostIntegration");

    send_control(
        &mut stream,
        MessageType::Invoke,
        81,
        &(InvokeControl {
            operation: InvokeOperation::HandleEndpoint,
            remaining_budget_ms: 5_000,
            operation_id: 1,
        }),
        serde_json
            ::to_vec(
                &serde_json::json!({
            "endpoint_id": "sandboxProbe",
            "method": "Get",
            "raw_path": "/pluginhost-integration/sandbox-probe",
            "base_path": "/",
            "raw_query": null,
            "content_type": null,
            "body": "",
            "principal": {
                "user_id": "sandbox-test",
                "username": "sandbox-test",
                "given_name": "Sandbox",
                "family_name": "Test",
                "groups": [],
                "session_id": "sandbox-session"
            }
        })
            )
            .expect("sandbox probe request must serialize")
    );
    let result = read_frame(&mut stream);
    assert_eq!(result.message_type, MessageType::Result);
    assert_eq!(result.request_id, 81);
    let result_control: ResultControl = result.decode_control().expect("Result must decode");
    assert!(result_control.success, "sandbox probe failed: {:?}", result_control.error_message);
    let wrapper: serde_json::Value = serde_json
        ::from_slice(&result.payload)
        .expect("wrapper payload must be JSON");
    let response_body: serde_json::Value = serde_json
        ::from_str(wrapper["Ok"]["body"].as_str().expect("endpoint body must be encoded as text"))
        .expect("endpoint body must be JSON");
    assert_eq!(
        response_body["network_denied"],
        true,
        "domain network socket creation must be denied: {response_body}"
    );
    assert_eq!(
        response_body["restricted_file_denied"],
        true,
        "unlisted filesystem paths must be denied: {response_body}"
    );
    assert_eq!(
        response_body["process_creation_denied"],
        true,
        "process creation must be denied: {response_body}"
    );
    assert_eq!(
        response_body["cross_process_signal_denied"],
        true,
        "plugins must not signal the backend or other processes: {response_body}"
    );
    assert_eq!(
        response_body["capability_mutation_denied"],
        true,
        "plugins must not mutate Linux capabilities: {response_body}"
    );
    assert_eq!(
        response_body["temp_write_allowed"],
        true,
        "the explicitly granted worker temp directory must remain writable: {response_body}"
    );

    send_control(
        &mut stream,
        MessageType::Shutdown,
        99,
        &(ShutdownControl { grace_budget_ms: 1_000 }),
        Vec::new()
    );
    let shutdown = read_frame(&mut stream);
    assert_eq!(shutdown.message_type, MessageType::ShutdownComplete);
    let shutdown: ShutdownCompleteControl = shutdown
        .decode_control()
        .expect("ShutdownComplete must decode");
    assert!(shutdown.graceful, "worker cleanup failed: {:?}", shutdown.message);
    assert_child_clean_exit(&mut child.0);
    drop(listener);
    let _ = fs::remove_dir_all(&sandbox_dir);
    Ok(())
}
