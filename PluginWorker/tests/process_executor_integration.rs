#![cfg(unix)]

//! End-to-end integration for Factory -> ProcessExecutor -> real worker process -> ABI V2 fixture.
//! This test is intentionally unconfined and must never be used as a sandbox acceptance test.

use DatabaseContracts::{ DatabaseError, DatabaseErrorKind, DatabaseRequest, DatabaseResponse };
use PluginContracts::ApiContract::{ PluginEndpointRequest, PluginHttpMethod, PluginPrincipal };
use PluginContracts::PluginAbi::{
    AbiBuffer,
    AbiSlice,
    PluginCancellationV2,
    PLUGIN_STATUS_INVALID_ARGUMENT,
    PLUGIN_STATUS_CANCELLED,
    PLUGIN_STATUS_OK,
};
use PluginHost::Factory::{ self, PluginExecutorKind, PluginRuntimeConfig };
use std::fs;
use std::path::{ Path, PathBuf };
use std::process::Command;
use std::sync::atomic::{ AtomicUsize, Ordering };
use std::sync::Mutex;

static TEST_LOCK: Mutex<()> = Mutex::new(());
static DB_EXECUTOR_CALLS: AtomicUsize = AtomicUsize::new(0);

fn fixture_manifest_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../PluginHost/tests/pluginhost_integration_fixture/Cargo.toml"
    )
}

fn fixture_library_name() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "libpluginhost_integration_fixture.so"
    }
    #[cfg(target_os = "macos")]
    {
        "libpluginhost_integration_fixture.dylib"
    }
}

fn build_real_plugin() -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let root = std::env
        ::temp_dir()
        .join(
            format!(
                "ezf-process-executor-fixture-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            )
        );
    let target = root.join("target");
    fs::create_dir_all(&target).expect("fixture target must be creatable");
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = Command::new(cargo)
        .arg("build")
        .arg("--manifest-path")
        .arg(fixture_manifest_path())
        .arg("--locked")
        .arg("--target-dir")
        .arg(&target)
        .arg("--quiet")
        .env_remove("EZF_PLUGIN_EXECUTOR")
        .env_remove("EZF_PLUGIN_ALLOW_UNCONFINED")
        .env_remove("EZF_PLUGIN_WORKER_PATH")
        .env_remove("EZF_PLUGIN_FIXTURE_MODE")
        .status()
        .expect("cargo must be available to build the ABI V2 fixture");
    assert!(status.success(), "the ABI V2 fixture must compile");
    target.join("debug").join(fixture_library_name())
}

struct EnvGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvGuard {
    fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(key);
        // This integration-test target contains a single test; environment changes are scoped to
        // this test process and do not affect other Cargo test binaries.
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        unsafe {
            match self.previous.as_ref() {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

fn ping_request() -> PluginEndpointRequest {
    PluginEndpointRequest {
        endpoint_id: "ping".to_owned(),
        method: PluginHttpMethod::Get,
        raw_path: "/pluginhost-integration/ping".to_owned(),
        base_path: "/".to_owned(),
        raw_query: None,
        content_type: None,
        body: Vec::new(),
        principal: PluginPrincipal {
            user_id: "process-integration-test".to_owned(),
            username: "process-integration-test".to_owned(),
            given_name: "Process".to_owned(),
            family_name: "Integration".to_owned(),
            groups: Vec::new(),
            session_id: "process-integration-session".to_owned(),
        },
    }
}

fn slow_request() -> PluginEndpointRequest {
    PluginEndpointRequest {
        endpoint_id: "slow".to_owned(),
        method: PluginHttpMethod::Get,
        raw_path: "/pluginhost-integration/slow".to_owned(),
        base_path: "/".to_owned(),
        raw_query: None,
        content_type: None,
        body: Vec::new(),
        principal: PluginPrincipal {
            user_id: "process-integration-test".to_owned(),
            username: "process-integration-test".to_owned(),
            given_name: "Process".to_owned(),
            family_name: "Integration".to_owned(),
            groups: Vec::new(),
            session_id: "process-integration-session".to_owned(),
        },
    }
}

#[test]
fn factory_uses_process_executor_for_all_runtime_calls_and_unloads_worker() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let worker = Path::new(env!("CARGO_BIN_EXE_ezf-plugin-worker"));
    assert!(worker.is_file(), "Cargo-built worker binary must exist: {}", worker.display());
    let _mode = EnvGuard::set("EZF_PLUGIN_EXECUTOR", "process");
    let _unconfined = EnvGuard::set("EZF_PLUGIN_ALLOW_UNCONFINED", "1");
    let _worker_path = EnvGuard::set("EZF_PLUGIN_WORKER_PATH", worker.as_os_str());
    let _fixture_mode = EnvGuard::set("EZF_PLUGIN_FIXTURE_MODE", "normal");

    let plugin = build_real_plugin();
    assert!(plugin.is_file(), "fixture library must exist: {}", plugin.display());

    let mut config = PluginRuntimeConfig::default();
    config.worker_threads = 4;
    config.max_concurrent_calls = 4;
    config.queue_capacity = 4;
    let id = Factory::load_plugin_file_with_config(&plugin, config).expect(
        "Factory must load the fixture through ProcessExecutor"
    );
    assert_eq!(Factory::loaded_plugin_names(), vec!["PluginHostIntegration"]);
    let diagnostics = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "loaded process plugin must have a health snapshot"
    );
    assert_eq!(diagnostics.executor, PluginExecutorKind::Process);
    assert_eq!(diagnostics.worker_threads, 4);
    assert_eq!(diagnostics.max_concurrent_calls, 4);
    assert_eq!(
        diagnostics.max_parallel_plugin_calls,
        1,
        "one process-backed worker must report single-flight plugin parallelism even when the host runtime has multiple worker threads"
    );
    let worker_pid = diagnostics.worker_pid.expect("live process executor must report worker PID");
    assert!(worker_pid > 0, "worker PID must be a valid non-zero process id");

    let response = Factory::handle_endpoint(ping_request()).expect(
        "the endpoint operation must cross the IPC/process boundary"
    );
    assert_eq!(response.status, 200);
    let body: serde_json::Value = serde_json
        ::from_slice(&response.body)
        .expect("process-backed endpoint response must contain JSON");
    assert_eq!(body["plugin"], "PluginHostIntegration");
    assert_eq!(body["endpoint"], "ping");
    assert_eq!(body["loaded"], true);

    // Despite four host-side runtime workers and four admitted-call slots, the current process
    // executor must run only one top-level plugin callback inside its single worker process.
    let callers = (0..2)
        .map(|_|
            std::thread::spawn(||
                Factory::handle_endpoint(slow_request()).expect(
                    "serialized slow endpoint call must succeed"
                )
            )
        )
        .collect::<Vec<_>>();
    for caller in callers {
        let response = caller.join().expect("concurrent caller thread must not panic");
        assert_eq!(response.status, 200);
        let body: serde_json::Value = serde_json
            ::from_slice(&response.body)
            .expect("slow endpoint response must contain JSON");
        assert_eq!(
            body["peak_active"],
            1,
            "one process worker must never execute two top-level plugin callbacks concurrently"
        );
    }

    Factory::unload_plugin(id).expect("ProcessExecutor must unload and reap the worker");
    assert!(Factory::loaded_plugin_names().is_empty());
    let _ = fs::remove_dir_all(
        plugin
            .parent()
            .and_then(Path::parent)
            .and_then(Path::parent)
            .unwrap_or_else(|| Path::new("/nonexistent"))
    );
}

unsafe extern "C" fn test_database_execute(
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

    let cancelled = unsafe { (cancellation.is_cancelled)(cancellation.context) };
    if cancelled {
        return PLUGIN_STATUS_CANCELLED;
    }

    let abi_request = unsafe { request.to_vec() };
    let Some(bytes) = abi_request else {
        return PLUGIN_STATUS_INVALID_ARGUMENT;
    };

    if serde_json::from_slice::<DatabaseRequest>(&bytes).is_err() {
        let result = Result::<DatabaseResponse, DatabaseError>::Err(
            DatabaseError::new(
                DatabaseErrorKind::InvalidRequest,
                "test host callback received invalid DatabaseRequest JSON"
            )
        );
        unsafe {
            *response_out = AbiBuffer::from_vec(serde_json::to_vec(&result).unwrap());
        }
        return PLUGIN_STATUS_OK;
    }

    DB_EXECUTOR_CALLS.fetch_add(1, Ordering::AcqRel);
    // Use the real Factory database route, which dispatches to a separately running DataSource
    // ProcessExecutor. This exercises both processes and the host broker rather than a local stub.
    let output = match Factory::execute_database_raw(bytes, cancellation) {
        Ok(response) => response,
        Err(error) =>
            serde_json
                ::to_vec(&Result::<DatabaseResponse, DatabaseError>::Err(error))
                .expect("database error envelope must serialize"),
    };
    unsafe {
        *response_out = AbiBuffer::from_vec(output);
    }
    PLUGIN_STATUS_OK
}

unsafe extern "C" fn test_database_free(buffer: AbiBuffer) {
    unsafe {
        buffer.free();
    }
}

#[test]
fn database_and_logging_capabilities_cross_ipc_without_deadlocking() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let worker = Path::new(env!("CARGO_BIN_EXE_ezf-plugin-worker"));
    assert!(worker.is_file(), "Cargo-built worker binary must exist: {}", worker.display());
    let _mode = EnvGuard::set("EZF_PLUGIN_EXECUTOR", "process");
    let _unconfined = EnvGuard::set("EZF_PLUGIN_ALLOW_UNCONFINED", "1");
    let _worker_path = EnvGuard::set("EZF_PLUGIN_WORKER_PATH", worker.as_os_str());
    let _fixture_mode = EnvGuard::set("EZF_PLUGIN_FIXTURE_MODE", "database_provider");

    Factory::register_database_executor(test_database_execute, test_database_free).expect(
        "host database test capability must register before process plugin load"
    );
    let plugin = build_real_plugin();
    let domain_plugin = plugin.with_file_name(
        format!(
            "domain-{}",
            plugin.file_name().expect("fixture path has a basename").to_string_lossy()
        )
    );
    fs::copy(&plugin, &domain_plugin).expect("Domain fixture library copy must succeed");
    let provider_id = Factory::load_plugin_file_with_config(
        &plugin,
        PluginRuntimeConfig::default()
    ).expect("DataSource fixture must load through ProcessExecutor");
    assert_eq!(Factory::loaded_plugin_names(), vec!["PluginHostDatabaseProvider"]);

    unsafe {
        std::env::set_var("EZF_PLUGIN_FIXTURE_MODE", "database_broker");
    }
    let id = Factory::load_plugin_file_with_config(
        &domain_plugin,
        PluginRuntimeConfig::default()
    ).expect("Domain fixture must load with the host database capability broker");
    assert!(Factory::loaded_plugin_names().contains(&"PluginHostDatabaseBroker".to_owned()));

    let mut request = ping_request();
    request.endpoint_id = "database".to_owned();
    request.raw_path = "/pluginhost-integration/database".to_owned();
    let response = Factory::handle_endpoint(request).expect(
        "database capability must complete through the IPC broker without deadlock"
    );
    assert_eq!(response.status, 200);
    let body: serde_json::Value = serde_json
        ::from_slice(&response.body)
        .expect("database broker response must be JSON");
    assert_eq!(body["endpoint"], "database");
    assert_eq!(body["rows"], 1);
    assert_eq!(body["loaded"], true);

    assert_eq!(
        DB_EXECUTOR_CALLS.load(Ordering::Acquire),
        1,
        "Domain plugin must cross the host DB callback once"
    );
    Factory::unload_plugin(id).expect("broker fixture worker must unload and be reaped");
    assert!(Factory::loaded_plugin_names().contains(&"PluginHostDatabaseProvider".to_owned()));
    Factory::unload_plugin(provider_id).expect(
        "DataSource provider worker must unload and be reaped"
    );
    assert!(Factory::loaded_plugin_names().is_empty());

    // The broker must fail closed for a DataSource plugin, otherwise it could recursively route
    // its own database request back to itself. The registered executor call count must not change.
    unsafe {
        std::env::set_var("EZF_PLUGIN_FIXTURE_MODE", "database_broker_datasource");
    }
    let datasource_id = Factory::load_plugin_file_with_config(
        &plugin,
        PluginRuntimeConfig::default()
    ).expect("DataSource fixture itself must load, while generic DB capability remains denied");
    assert_eq!(Factory::loaded_plugin_names(), vec!["PluginHostDatabaseBrokerDataSource"]);
    let mut denied_request = ping_request();
    denied_request.endpoint_id = "database".to_owned();
    denied_request.raw_path = "/pluginhost-integration/database".to_owned();
    // A plugin endpoint error is transported as a typed PluginEndpointError, so the
    // Factory returns Err rather than a successful PluginEndpointResponse. Capture the
    // result first and unload the worker before asserting: even if the expected policy
    // shape changes in a later revision, this test must not leave a process or catalog
    // entry behind and cascade failures into the next integration test.
    let denied_result = Factory::handle_endpoint(denied_request);
    Factory::unload_plugin(datasource_id).expect(
        "DataSource fixture worker must unload and be reaped"
    );
    assert!(Factory::loaded_plugin_names().is_empty());

    let denied = denied_result.expect_err(
        "DataSource capability denial must be reported as a typed plugin endpoint error, not an IPC transport failure"
    );
    assert_eq!(denied.status, 502);
    assert_eq!(denied.code, "database_broker_failed");
    assert!(denied.user_message.contains("database capability denied for DataSource plugin"));
    assert_eq!(
        DB_EXECUTOR_CALLS.load(Ordering::Acquire),
        1,
        "DataSource request must be denied before invoking the host DB callback"
    );
    let _ = fs::remove_dir_all(
        plugin
            .parent()
            .and_then(Path::parent)
            .and_then(Path::parent)
            .unwrap_or_else(|| Path::new("/nonexistent"))
    );
}
