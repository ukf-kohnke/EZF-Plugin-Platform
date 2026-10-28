use PluginContracts::ApiContract::{
    PluginEndpointRequest,
    PluginHttpMethod,
    PluginPrincipal,
    PluginSessionContext,
};
use PluginHost::Factory::{
    self,
    PluginHealthState,
    PluginLoadPolicy,
    PluginManagerError,
    PluginRequirement,
    PluginRuntimeConfig,
};
use std::{
    collections::BTreeSet,
    fs,
    path::PathBuf,
    process::Command,
    sync::{ atomic::{ AtomicUsize, Ordering }, Mutex, MutexGuard },
};

fn fixture_manifest_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "tests/pluginhost_integration_fixture/Cargo.toml"
    )
}

fn fixture_package_target_name() -> &'static str {
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
}

fn build_fixture() -> PathBuf {
    let manifest = fixture_manifest_path();
    assert!(manifest.is_file(), "dedicated PluginHost test-plugin manifest must exist");

    static BUILD_COUNTER: AtomicUsize = AtomicUsize::new(0);
    let build_id = BUILD_COUNTER.fetch_add(1, Ordering::Relaxed);
    let temp_dir = std::env
        ::temp_dir()
        .join(format!("ezf-pluginhost-dylib-{}-{}", std::process::id(), build_id));
    let target_dir = temp_dir.join("target");
    fs::create_dir_all(&target_dir).expect("fixture target directory must be creatable");

    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = Command::new(cargo)
        .env_remove("EZF_TEST_PLUGIN_NAME")
        .arg("build")
        .arg("--manifest-path")
        .arg(&manifest)
        .arg("--target-dir")
        .arg(&target_dir)
        .arg("--quiet")
        .status()
        .expect("cargo must be available to build the dedicated dynamic test plugin");
    assert!(status.success(), "dedicated PluginHost test-plugin failed to build");

    target_dir.join("debug").join(fixture_package_target_name())
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
            user_id: "integration-test".to_owned(),
            username: "integration-test".to_owned(),
            given_name: "Integration".to_owned(),
            family_name: "Test".to_owned(),
            groups: Vec::new(),
            session_id: "integration-session".to_owned(),
        },
    }
}

fn held_slow_request() -> PluginEndpointRequest {
    let mut request = ping_request();
    request.endpoint_id = "slow".to_owned();
    request.raw_path = "/pluginhost-integration/slow".to_owned();
    request.raw_query = Some("hold=true".to_owned());
    request
}

static TEST_LOCK: Mutex<()> = Mutex::new(());

struct TestGuard {
    // Drop this guard before releasing the test lock so another test cannot observe a transient
    // executor mode while the process-global registry is being reused.
    _executor_mode: EnvironmentVariableGuard,
    _lock: MutexGuard<'static, ()>,
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        // Dynamic-library tests share PluginHost's process-global registry. Always tear down
        // anything left by a failed test so one panic cannot contaminate later tests.
        for _ in 0..3 {
            let ids = Factory::loaded_plugin_ids();
            if ids.is_empty() {
                break;
            }
            let mut remaining = false;
            for id in ids {
                if Factory::unload_plugin(id).is_err() {
                    remaining = true;
                }
            }
            if !remaining {
                break;
            }
        }
    }
}

fn lock_test() -> TestGuard {
    let lock = TEST_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    // This integration target exercises the in-process ABI/runtime semantics (parallel callbacks,
    // local cancellation pointers, and synchronous lifecycle callbacks). Pin the backend rather
    // than inheriting a shell-wide EZF_PLUGIN_EXECUTOR=process setting. Dedicated ProcessExecutor
    // coverage lives in PluginWorker/tests and selects `process` explicitly there.
    let executor_mode = EnvironmentVariableGuard::set("EZF_PLUGIN_EXECUTOR", "in_process");
    TestGuard {
        _executor_mode: executor_mode,
        _lock: lock,
    }
}

fn cleanup_plugin(id: i64) {
    Factory::unload_plugin(id).expect("test plugin must unload cleanly");
}

struct EnvironmentVariableGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvironmentVariableGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(key);
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }
}

impl Drop for EnvironmentVariableGuard {
    fn drop(&mut self) {
        unsafe {
            match self.previous.as_ref() {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

#[test]
fn indexed_dispatch_rejects_an_endpoint_id_mismatch() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let id = Factory::load_plugin_file(&library).expect("fixture library must load");

    let mut request = ping_request();
    request.endpoint_id = "not-ping".to_owned();
    let error = Factory::handle_endpoint(request).expect_err(
        "a known method/path must not dispatch under a different endpoint contract id"
    );

    assert_eq!(error.code, "plugin_endpoint_contract_mismatch");
    cleanup_plugin(id);
    let _ = fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn loads_executes_unloads_and_reloads_a_real_dynamic_library() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let id = Factory::load_plugin_file(&library).expect("fixture library must load");

    assert_eq!(Factory::plugin_count(), 1);
    assert_eq!(Factory::loaded_plugin_names(), vec!["PluginHostIntegration"]);

    let initial_health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "loaded fixture health must be available before productive calls"
    );
    assert_eq!(initial_health.total_calls, 0);
    assert_eq!(initial_health.completed_calls, 0);
    assert_eq!(initial_health.failed_calls, 0);
    assert_eq!(initial_health.timed_out_calls, 0);
    assert_eq!(initial_health.timed_out_active_calls, 0);

    let response = Factory::handle_endpoint(ping_request()).expect(
        "dynamic endpoint call must cross the ABI boundary"
    );
    assert_eq!(response.status, 200);
    let body: serde_json::Value = serde_json
        ::from_slice(&response.body)
        .expect("plugin response must contain valid JSON");
    assert_eq!(body["plugin"], "PluginHostIntegration");
    assert_eq!(body["loaded"], true);
    assert_eq!(body["endpoint"], "ping");

    let health = Factory::plugin_health_by_name("pluginhostintegration").expect(
        "loaded fixture health must be available"
    );
    assert_eq!(health.state, PluginHealthState::Ready);
    assert!(health.total_calls >= 1);
    assert_eq!(health.failed_calls, 0);
    Factory::check_readiness().expect("healthy loaded plugin must pass the readiness gate");

    cleanup_plugin(id);
    assert_eq!(Factory::plugin_count(), 0);

    let reloaded_id = Factory::load_plugin_file(&library).expect(
        "the same dynamic library must be reloadable after unload"
    );
    assert_eq!(Factory::plugin_count(), 1);

    let reloaded_response = Factory::handle_endpoint(ping_request()).expect(
        "reloaded dynamic endpoint call must cross the ABI boundary"
    );
    assert_eq!(reloaded_response.status, 200);
    let reloaded_body: serde_json::Value = serde_json
        ::from_slice(&reloaded_response.body)
        .expect("reloaded plugin response must contain valid JSON");
    assert_eq!(reloaded_body["plugin"], "PluginHostIntegration");
    assert_eq!(reloaded_body["loaded"], true);
    assert_eq!(reloaded_body["endpoint"], "ping");

    cleanup_plugin(reloaded_id);

    let first_path = library.clone();
    let second_path = library.clone();
    let first = std::thread::spawn(move || Factory::load_plugin_file(&first_path));
    let second = std::thread::spawn(move || Factory::load_plugin_file(&second_path));
    let first_result = first.join().expect("first loader thread must finish");
    let second_result = second.join().expect("second loader thread must finish");

    let results = [first_result, second_result];
    let successful_id = results
        .iter()
        .find_map(|result| result.as_ref().ok().copied())
        .expect("exactly one concurrent load must succeed");
    assert!(
        results
            .iter()
            .filter(|result| result.is_ok())
            .count() == 1
    );
    assert!(
        results
            .iter()
            .any(|result|
                matches!(
                    result,
                    Err(PluginManagerError::PluginAlreadyLoaded { .. }) |
                        Err(PluginManagerError::PluginLoadInProgress { .. })
                )
            )
    );
    cleanup_plugin(successful_id);
    if let Some(temp_dir) = library.parent() {
        let _ = fs::remove_dir_all(temp_dir);
    }
}

#[test]
fn loading_is_deterministic_and_optional_failures_are_reported_without_aborting() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let temp = std::env
        ::temp_dir()
        .join(format!("ezf-pluginhost-load-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&temp);
    fs::create_dir_all(&temp).expect("test directory must be creatable");

    #[cfg(target_os = "linux")]
    let library_name = "PluginHostIntegration.so";
    #[cfg(target_os = "macos")]
    let library_name = "PluginHostIntegration.dylib";
    #[cfg(target_os = "windows")]
    let library_name = "PluginHostIntegration.dll";

    let good_copy = temp.join(library_name);
    fs::copy(&library, &good_copy).expect("fixture library must be copied");
    fs::write(
        temp.join(match std::env::consts::DLL_EXTENSION {
            "dll" => "broken.dll",
            "dylib" => "broken.dylib",
            _ => "broken.so",
        }),
        b"this is not a dynamic library"
    ).expect("broken fixture must be written");

    let report = Factory::load_plugins_with_policy(
        temp.to_str().expect("temporary path must be UTF-8"),
        PluginLoadPolicy::optional_all().require("PluginHostIntegration")
    ).expect("required fixture should load while optional broken plugin is tolerated");

    assert_eq!(report.loaded, vec!["PluginHostIntegration"]);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].requirement, PluginRequirement::Optional);
    assert_eq!(report.failures[0].plugin_name, None);
    assert!(report.discovered.windows(2).all(|pair| pair[0] <= pair[1]));

    let loaded_id = Factory::loaded_plugin_ids()
        .into_iter()
        .next()
        .expect("loaded fixture id must exist");
    cleanup_plugin(loaded_id);
    let _ = fs::remove_dir_all(&temp);

    let empty = std::env
        ::temp_dir()
        .join(format!("ezf-pluginhost-required-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&empty);
    fs::create_dir_all(&empty).expect("required test directory must be creatable");

    let error = Factory::load_plugins_with_policy(
        empty.to_str().expect("temporary path must be UTF-8"),
        PluginLoadPolicy::optional_all().require("MissingRequiredPlugin")
    ).expect_err("missing required plugin must abort the load operation");

    match error {
        PluginManagerError::PluginLoadPolicyViolation { report } => {
            assert!(report.has_required_failures());
            assert_eq!(report.required_failures().len(), 1);
            assert_eq!(
                report.required_failures()[0].plugin_name.as_deref(),
                Some("missingrequiredplugin")
            );
        }
        other => panic!("unexpected plugin load error: {other}"),
    }

    let _ = fs::remove_dir_all(&empty);
}

#[test]
fn plugin_calls_reuse_a_bounded_persistent_worker_pool() {
    // The fixture is a separately compiled cdylib. Use a fixture-local TLS id to detect reuse of
    // the same underlying worker thread without relying on Rust thread-name metadata across the ABI boundary.
    let _test_guard = lock_test();
    let library = build_fixture();
    let config = PluginRuntimeConfig {
        call_timeout: std::time::Duration::from_secs(1),
        unload_timeout: std::time::Duration::from_secs(2),
        lifecycle_timeout: std::time::Duration::from_secs(2),
        worker_threads: 2,
        max_concurrent_calls: 2,
        queue_capacity: 2,
    };
    let id = Factory::load_plugin_file_with_config(&library, config).expect(
        "fixture library must load with a bounded worker pool"
    );

    let mut worker_threads = BTreeSet::new();
    for iteration in 0..100 {
        let response = Factory::handle_endpoint(ping_request()).unwrap_or_else(|error|
            panic!("iteration {iteration}: ping failed: {error}")
        );
        assert_eq!(response.status, 200);
        let body: serde_json::Value = serde_json
            ::from_slice(&response.body)
            .expect("ping response must contain valid JSON");
        let worker_thread_id = body["worker_thread_id"]
            .as_u64()
            .expect("ping response must report the persistent worker thread id");
        worker_threads.insert(worker_thread_id);
    }

    assert!(
        worker_threads.len() <= 2,
        "bounded worker pool must reuse at most 2 persistent worker threads, observed {:?}",
        worker_threads
    );

    cleanup_plugin(id);
    let _ = std::fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn plugin_name_and_manifest_use_the_bounded_timeout_execution_path() {
    let _test_guard = lock_test();

    for mode in ["slow_name", "slow_manifest"] {
        let _environment = EnvironmentVariableGuard::set("EZF_PLUGIN_FIXTURE_MODE", mode);
        let library = build_fixture();
        let config = PluginRuntimeConfig {
            call_timeout: std::time::Duration::from_millis(25),
            unload_timeout: std::time::Duration::from_secs(1),
            lifecycle_timeout: std::time::Duration::from_secs(1),
            worker_threads: 1,
            max_concurrent_calls: 1,
            queue_capacity: 1,
        };

        let result = Factory::load_plugin_file_with_config(&library, config);

        let error = result.expect_err("slow plugin metadata call must time out during loading");
        match error {
            PluginManagerError::PluginAbiCall { message, .. } => {
                assert!(
                    message.contains("timed out"),
                    "metadata call must report the configured timeout, got: {message}"
                );
            }
            other => panic!("unexpected metadata timeout error: {other}"),
        }

        assert!(
            Factory::loaded_plugin_ids().is_empty(),
            "a timed-out metadata call must not register a plugin"
        );

        let _ = std::fs::remove_dir_all(library.parent().unwrap());
    }
}

#[test]
fn plugin_calls_execute_in_parallel_without_serializing_on_runtime_lock() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let id = Factory::load_plugin_file(&library).expect("fixture library must load");

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
                user_id: "integration-test".to_owned(),
                username: "integration-test".to_owned(),
                given_name: "Integration".to_owned(),
                family_name: "Test".to_owned(),
                groups: Vec::new(),
                session_id: "integration-session".to_owned(),
            },
        }
    }

    let first = std::thread::spawn(move || {
        Factory::handle_endpoint(slow_request()).expect(
            "first concurrent endpoint call must succeed"
        )
    });
    let second = std::thread::spawn(move || {
        Factory::handle_endpoint(slow_request()).expect(
            "second concurrent endpoint call must succeed"
        )
    });

    let first_response = first.join().expect("first endpoint thread must finish");
    let second_response = second.join().expect("second endpoint thread must finish");

    for response in [first_response, second_response] {
        assert_eq!(response.status, 200);
        let body: serde_json::Value = serde_json
            ::from_slice(&response.body)
            .expect("slow endpoint response must contain valid JSON");
        assert!(body["peak_active"].as_u64().unwrap_or_default() >= 2);
    }

    cleanup_plugin(id);
    let _ = std::fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn concurrent_abi_calls_keep_context_alive_until_unload_drains_every_call() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let config = PluginRuntimeConfig {
        call_timeout: std::time::Duration::from_secs(5),
        unload_timeout: std::time::Duration::from_secs(3),
        lifecycle_timeout: std::time::Duration::from_secs(2),
        worker_threads: 2,
        max_concurrent_calls: 2,
        queue_capacity: 2,
    };
    let id = Factory::load_plugin_file_with_config(&library, config).expect(
        "Rust ABI fixture must load with concurrent execution enabled"
    );
    let plugin_dir = library.parent().unwrap();
    let release_marker = plugin_dir.join("slow-release.marker");
    let second_active_marker = plugin_dir.join("slow-active-2.marker");
    let destroy_marker = plugin_dir.join("destroy.marker");
    for marker in [&release_marker, &second_active_marker, &destroy_marker] {
        let _ = fs::remove_file(marker);
    }

    let first = std::thread::spawn(|| Factory::handle_endpoint(held_slow_request()));
    let second = std::thread::spawn(|| Factory::handle_endpoint(held_slow_request()));

    let active_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !second_active_marker.is_file() {
        if first.is_finished() || second.is_finished() {
            let _ = fs::write(&release_marker, b"release");
            let _ = first.join();
            let _ = second.join();
            panic!("both ABI callbacks must be active before unload begins");
        }
        if std::time::Instant::now() >= active_deadline {
            let _ = fs::write(&release_marker, b"release");
            let _ = first.join();
            let _ = second.join();
            panic!("second concurrent callback did not enter the shared plugin context");
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    let unload_thread = std::thread::spawn(move || Factory::unload_plugin(id));
    let stopping_deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut observed_stopping = false;
    while std::time::Instant::now() < stopping_deadline {
        if
            Factory::plugin_health_by_name("PluginHostIntegration").is_some_and(
                |snapshot| snapshot.state == PluginHealthState::Stopping
            )
        {
            observed_stopping = true;
            break;
        }
        std::thread::yield_now();
    }

    // If destroy were invoked before both callbacks return, this marker would appear while the
    // context is still in use. Always release callbacks before assertions to avoid stranded workers.
    let destroyed_while_active = destroy_marker.is_file();
    fs::write(&release_marker, b"release").expect("held ABI callbacks must be released");

    let unload_result = unload_thread.join().expect("unload worker must finish");
    let first_result = first.join().expect("first ABI callback must finish");
    let second_result = second.join().expect("second ABI callback must finish");

    assert!(observed_stopping, "unload must expose stopping while calls are active");
    assert!(!destroyed_while_active, "destroy must not run while an ABI callback is active");
    assert!(first_result.is_err(), "first callback should observe unload cancellation");
    assert!(second_result.is_err(), "second callback should observe unload cancellation");
    unload_result.expect("unload must finish after all concurrent ABI callbacks drain");
    assert_eq!(Factory::plugin_count(), 0);
    assert!(destroy_marker.is_file(), "destroy must run after the calls have drained");
    let _ = fs::remove_dir_all(plugin_dir);
}

#[test]
fn timed_plugin_calls_return_without_detaching_the_running_library() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let config = PluginRuntimeConfig {
        call_timeout: std::time::Duration::from_millis(50),
        unload_timeout: std::time::Duration::from_secs(2),
        lifecycle_timeout: std::time::Duration::from_secs(2),
        worker_threads: 2,
        max_concurrent_calls: 2,
        queue_capacity: 2,
    };
    let id = Factory::load_plugin_file_with_config(&library, config).expect(
        "fixture library must load with custom runtime limits"
    );

    let response = Factory::handle_endpoint({ PluginEndpointRequest {
            endpoint_id: "slow".to_owned(),
            method: PluginHttpMethod::Get,
            raw_path: "/pluginhost-integration/slow".to_owned(),
            base_path: "/".to_owned(),
            raw_query: None,
            content_type: None,
            body: Vec::new(),
            principal: PluginPrincipal {
                user_id: "integration-test".to_owned(),
                username: "integration-test".to_owned(),
                given_name: "Integration".to_owned(),
                family_name: "Test".to_owned(),
                groups: Vec::new(),
                session_id: "integration-session".to_owned(),
            },
        } }).expect_err("slow plugin call must exceed the custom timeout");

    assert_eq!(response.status, 504);
    assert_eq!(response.code, "plugin_timeout");
    let health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "timed-out fixture health must be available"
    );
    assert_eq!(health.state, PluginHealthState::Degraded);
    assert_eq!(health.timed_out_calls, 1);
    assert_eq!(health.timed_out_active_calls, 1);
    assert_eq!(health.completed_calls, 0);
    assert_eq!(health.failed_calls, 0);
    assert_eq!(health.total_calls, 1);
    assert_eq!(health.active_calls, 1);

    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(
        library.parent().unwrap().join("slow-cancelled.marker").is_file(),
        "the timed-out call must observe the cooperative cancellation request"
    );
    std::thread::sleep(std::time::Duration::from_millis(50));
    let recovered = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "timed-out plugin health must still be available after completion"
    );
    assert_eq!(recovered.state, PluginHealthState::Ready);
    assert_eq!(recovered.timed_out_active_calls, 0);
    assert_eq!(recovered.completed_calls, 1);
    assert_eq!(recovered.failed_calls, 0);
    assert_eq!(recovered.total_calls, 1);
    assert_eq!(recovered.active_calls, 0);
    assert!(recovered.last_error.is_none());

    Factory::unload_plugin(id).expect("library must remain safely unloadable after timeout");
    assert_eq!(Factory::plugin_count(), 0);
    let _ = std::fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn unload_timeout_keeps_the_plugin_registered_until_quiescing_succeeds() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let config = PluginRuntimeConfig {
        call_timeout: std::time::Duration::from_secs(2),
        unload_timeout: std::time::Duration::from_millis(25),
        lifecycle_timeout: std::time::Duration::from_millis(25),
        worker_threads: 2,
        max_concurrent_calls: 2,
        queue_capacity: 2,
    };
    let previous = fixture_mode(Some("slow_ignore_cancel"));
    let id = Factory::load_plugin_file_with_config(&library, config).expect(
        "fixture library must load with bounded unload timeout"
    );
    restore_fixture_mode(previous);

    let request = PluginEndpointRequest {
        endpoint_id: "slow".to_owned(),
        method: PluginHttpMethod::Get,
        raw_path: "/pluginhost-integration/slow".to_owned(),
        base_path: "/".to_owned(),
        raw_query: None,
        content_type: None,
        body: Vec::new(),
        principal: PluginPrincipal {
            user_id: "integration-test".to_owned(),
            username: "integration-test".to_owned(),
            given_name: "Integration".to_owned(),
            family_name: "Test".to_owned(),
            groups: Vec::new(),
            session_id: "integration-session".to_owned(),
        },
    };

    let worker = std::thread::spawn(move || {
        Factory::handle_endpoint(request).expect(
            "slow endpoint call must remain valid while unload is blocked"
        )
    });

    let marker = library.parent().unwrap().join("slow-entered.marker");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !marker.is_file() {
        assert!(std::time::Instant::now() < deadline, "slow endpoint did not signal entry");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    let error = Factory::unload_plugin(id).expect_err(
        "unload must respect the configured quiescing timeout"
    );
    assert!(matches!(error, PluginManagerError::PluginUnloadFailed { .. }));
    assert_eq!(Factory::plugin_count(), 1);
    let health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "plugin must stay registered after bounded unload failure"
    );
    assert_eq!(health.state, PluginHealthState::Degraded);

    let response = worker.join().expect("slow endpoint worker must finish");
    assert_eq!(response.status, 200);
    Factory::unload_plugin(id).expect("plugin must be unloadable after the call drains");
    assert_eq!(Factory::plugin_count(), 0);
    let _ = std::fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn unload_requests_cooperative_cancellation_before_destroying_the_instance() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let id = Factory::load_plugin_file(&library).expect("fixture library must load");

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
                user_id: "integration-test".to_owned(),
                username: "integration-test".to_owned(),
                given_name: "Integration".to_owned(),
                family_name: "Test".to_owned(),
                groups: Vec::new(),
                session_id: "integration-session".to_owned(),
            },
        }
    }

    let marker = library.parent().unwrap().join("slow-entered.marker");
    let _ = std::fs::remove_file(&marker);

    let worker = std::thread::spawn(move || {
        Factory::handle_endpoint(slow_request()).expect_err(
            "the in-flight endpoint call must be cooperatively cancelled during unload"
        )
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !marker.is_file() {
        assert!(std::time::Instant::now() < deadline, "slow endpoint did not signal entry");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    let started = std::time::Instant::now();
    Factory::unload_plugin(id).expect("unload must wait for the active call");
    let elapsed = started.elapsed();

    let error = worker.join().expect("endpoint thread must finish");
    assert!(
        elapsed < std::time::Duration::from_millis(150),
        "unload did not observe cooperative cancellation promptly: {:?}",
        elapsed
    );
    assert!(
        library.parent().unwrap().join("slow-cancelled.marker").is_file(),
        "the in-flight endpoint callback must observe unload cancellation"
    );
    assert_eq!(error.status, 499);
    assert_eq!(error.code, "plugin_cancelled");
    assert_eq!(Factory::plugin_count(), 0);

    let _ = std::fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn p1_session_lifecycle_is_delivered_and_failures_are_reported() {
    let _test_guard = lock_test();

    let library = build_fixture();
    let id = Factory::load_plugin_file(&library).expect("fixture library must load");
    let context = PluginSessionContext::new("p1-session-123");

    let report = Factory::notify_session_closed(&context);
    assert_eq!(report.notified_plugins, vec!["PluginHostIntegration"]);
    assert!(report.failures.is_empty());
    assert!(
        library.parent().unwrap().join("session-closed-p1-session-123.marker").is_file(),
        "the dynamic plugin must receive the sanitized session-close callback"
    );

    cleanup_plugin(id);
    let _ = fs::remove_dir_all(library.parent().unwrap());

    let library = build_fixture();
    let previous = fixture_mode(Some("session_failure"));
    let id = Factory::load_plugin_file(&library).expect("failure fixture must load");
    restore_fixture_mode(previous);

    let report = Factory::notify_session_closed(&PluginSessionContext::new("p1-session-failure"));
    assert_eq!(report.notified_plugins, vec!["PluginHostIntegration"]);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].plugin_name, "PluginHostIntegration");
    assert_eq!(report.failures[0].error.code, "fixture_session_failure");

    cleanup_plugin(id);
    let _ = fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn p1_readiness_state_matrix_covers_ready_stopping_failed_and_empty() {
    let _test_guard = lock_test();

    let library = build_fixture();
    let config = PluginRuntimeConfig {
        call_timeout: std::time::Duration::from_secs(2),
        unload_timeout: std::time::Duration::from_secs(2),
        lifecycle_timeout: std::time::Duration::from_secs(2),
        worker_threads: 2,
        max_concurrent_calls: 2,
        queue_capacity: 2,
    };
    let id = Factory::load_plugin_file_with_config(&library, config).expect(
        "fixture library must load"
    );
    assert_eq!(
        Factory::plugin_health_by_name("PluginHostIntegration").unwrap().state,
        PluginHealthState::Ready
    );
    Factory::check_readiness().expect("ready plugin must pass readiness");

    let marker = library.parent().unwrap().join("slow-entered.marker");
    let _ = fs::remove_file(&marker);
    let worker = std::thread::spawn(|| {
        let mut request = ping_request();
        request.endpoint_id = "slow".to_owned();
        request.raw_path = "/pluginhost-integration/slow".to_owned();
        Factory::handle_endpoint(request).expect_err(
            "slow endpoint must observe cooperative unload cancellation"
        )
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !marker.is_file() {
        assert!(std::time::Instant::now() < deadline, "slow endpoint did not signal entry");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    let unload_thread = std::thread::spawn(move || Factory::unload_plugin(id));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut observed_stopping = false;
    while std::time::Instant::now() < deadline {
        if let Some(health) = Factory::plugin_health_by_name("PluginHostIntegration") {
            if health.state == PluginHealthState::Stopping {
                observed_stopping = true;
                assert!(Factory::check_readiness().is_err(), "stopping plugin must fail readiness");
                break;
            }
        }
        std::thread::yield_now();
    }
    assert!(observed_stopping, "readiness matrix must expose the transient stopping state");

    let cancellation_error = worker.join().expect("slow endpoint worker must finish");
    assert_eq!(cancellation_error.code, "plugin_cancelled");
    unload_thread.join().expect("unload worker must finish").expect("unload must succeed");
    assert!(Factory::check_readiness().is_ok(), "empty plugin set is ready for the host gate");

    let library = build_fixture();
    let previous = fixture_mode(Some("on_unload_failure"));
    let id = Factory::load_plugin_file(&library).expect("failure fixture must load");
    restore_fixture_mode(previous);

    let unload_error = Factory::unload_plugin(id).expect_err("first unload must fail");
    assert!(matches!(unload_error, PluginManagerError::PluginUnloadFailed { .. }));
    let health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "failed plugin must remain registered for readiness inspection"
    );
    assert_eq!(health.state, PluginHealthState::Failed);
    assert!(Factory::check_readiness().is_err(), "failed plugin must fail readiness");

    Factory::unload_plugin(id).expect("retry after a failed unload must succeed");
    assert!(Factory::check_readiness().is_ok(), "empty host must pass the readiness gate");
    let _ = fs::remove_dir_all(library.parent().unwrap());
}

#[cfg(unix)]
#[test]
fn p1_discovery_symlink_and_path_aliases_are_canonicalized_once() {
    let _test_guard = lock_test();

    let library = build_fixture();
    let actual_dir = library.parent().unwrap().to_path_buf();
    let root = std::env
        ::temp_dir()
        .join(format!("ezf-pluginhost-p1-discovery-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("discovery root must be creatable");

    let alias_dir = root.join("alias");
    std::os::unix::fs::symlink(&actual_dir, &alias_dir).expect("unix directory symlink must work");

    let report = Factory::load_plugins_with_policy(
        alias_dir.to_str().unwrap(),
        PluginLoadPolicy::required_all()
    ).expect("aliased discovery must load the required plugin");

    assert_eq!(report.loaded, vec!["PluginHostIntegration"]);
    assert_eq!(report.discovered.len(), 1);
    assert_eq!(report.discovered[0], library.canonicalize().unwrap());
    assert!(report.failures.is_empty());

    let loaded_id = Factory::loaded_plugin_ids()
        .into_iter()
        .next()
        .expect("discovery must register the fixture");

    let lexical_alias = alias_dir.join(".").join(library.file_name().unwrap());
    let duplicate = Factory::load_plugin_file(&lexical_alias).expect_err(
        "a lexical/symlink path alias must not load a second instance"
    );
    assert!(
        matches!(
        duplicate,
        PluginManagerError::PluginAlreadyLoaded { id, .. } if id == loaded_id
    )
    );

    cleanup_plugin(loaded_id);
    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn p1_timeout_recovers_to_ready_without_detaching_the_plugin() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let config = PluginRuntimeConfig {
        call_timeout: std::time::Duration::from_millis(50),
        unload_timeout: std::time::Duration::from_secs(2),
        lifecycle_timeout: std::time::Duration::from_secs(2),
        worker_threads: 2,
        max_concurrent_calls: 2,
        queue_capacity: 2,
    };
    let id = Factory::load_plugin_file_with_config(&library, config).expect(
        "fixture library must load with a short call timeout"
    );

    let mut slow = ping_request();
    slow.endpoint_id = "slow".to_owned();
    slow.raw_path = "/pluginhost-integration/slow".to_owned();
    let error = Factory::handle_endpoint(slow).expect_err(
        "slow endpoint must exceed the configured timeout"
    );
    assert_eq!(error.status, 504);
    assert_eq!(error.code, "plugin_timeout");

    let degraded = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "timed out plugin health must be available"
    );
    assert_eq!(degraded.state, PluginHealthState::Degraded);
    assert_eq!(degraded.timed_out_calls, 1);
    assert_eq!(degraded.timed_out_active_calls, 1);
    assert_eq!(degraded.completed_calls, 0);
    assert_eq!(degraded.failed_calls, 0);
    assert_eq!(degraded.active_calls, 1);
    assert!(Factory::check_readiness().is_err(), "degraded plugin must fail readiness");

    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(library.parent().unwrap().join("slow-cancelled.marker").is_file());
    std::thread::sleep(std::time::Duration::from_millis(50));

    let recovered = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "recovered plugin health must be available"
    );
    assert_eq!(recovered.state, PluginHealthState::Ready);
    assert_eq!(recovered.timed_out_active_calls, 0);
    assert_eq!(recovered.completed_calls, 1);
    assert_eq!(recovered.failed_calls, 0);
    assert_eq!(recovered.total_calls, 1);
    assert_eq!(recovered.active_calls, 0);
    assert!(recovered.last_error.is_none());
    assert!(Factory::check_readiness().is_ok(), "completed timeout must recover readiness");
    assert!(Factory::check_readiness().is_ok(), "recovered plugin must pass readiness");

    cleanup_plugin(id);
    assert_eq!(Factory::plugin_count(), 0);
    let _ = fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn p1_lifetime_ordering_is_on_unload_before_destroy_and_after_cooperative_cancellation() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let id = Factory::load_plugin_file(&library).expect("fixture library must load");

    let marker = library.parent().unwrap().join("slow-entered.marker");
    let _ = fs::remove_file(&marker);
    let exited_marker = library.parent().unwrap().join("slow-exited.marker");
    let _ = fs::remove_file(&exited_marker);
    let mut request = ping_request();
    request.endpoint_id = "slow".to_owned();
    request.raw_path = "/pluginhost-integration/slow".to_owned();

    let worker = std::thread::spawn(move || {
        Factory::handle_endpoint(request).expect_err(
            "in-flight request must observe cooperative cancellation during unload"
        )
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !marker.is_file() {
        assert!(std::time::Instant::now() < deadline, "slow endpoint did not signal entry");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    let started = std::time::Instant::now();
    Factory::unload_plugin(id).expect("plugin unload must succeed after quiescing");
    let elapsed = started.elapsed();
    assert!(
        library.parent().unwrap().join("slow-cancelled.marker").is_file(),
        "the in-flight plugin callback must observe unload cancellation"
    );
    assert!(
        !exited_marker.is_file(),
        "a cooperatively cancelled callback must not run to normal completion"
    );
    let response = worker.join().expect("request worker must finish");
    assert_eq!(response.status, 499);
    assert_eq!(response.code, "plugin_cancelled");
    assert!(
        elapsed < std::time::Duration::from_millis(150),
        "unload should not wait for the full 200 ms fixture operation: {:?}",
        elapsed
    );

    let order_path = library.parent().unwrap().join("lifecycle.order");
    let order = fs::read_to_string(&order_path).expect("lifecycle order log must exist");
    let events: Vec<&str> = order.lines().collect();
    assert_eq!(events, vec!["on_load", "on_unload", "on_destroy"]);
    assert!(library.parent().unwrap().join("unload.marker").is_file());
    assert!(library.parent().unwrap().join("destroy.marker").is_file());
    assert_eq!(Factory::plugin_count(), 0);

    let _ = fs::remove_dir_all(library.parent().unwrap());
}

fn build_malformed_fixture() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "tests/pluginhost_malformed_abi_fixture/Cargo.toml"
    );
    assert!(manifest.is_file(), "malformed ABI fixture manifest must exist");

    let temp_dir = std::env
        ::temp_dir()
        .join(format!("ezf-pluginhost-malformed-{}", std::process::id()));
    let target_dir = temp_dir.join("target");
    fs::create_dir_all(&target_dir).expect("malformed fixture target directory must be creatable");

    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = Command::new(cargo)
        .arg("build")
        .arg("--manifest-path")
        .arg(&manifest)
        .arg("--target-dir")
        .arg(&target_dir)
        .arg("--quiet")
        .status()
        .expect("cargo must be available to build the malformed ABI fixture");
    assert!(status.success(), "malformed ABI fixture failed to build");

    target_dir.join("debug").join(match std::env::consts::DLL_EXTENSION {
        "dll" => "pluginhost_malformed_abi_fixture.dll",
        "dylib" => "libpluginhost_malformed_abi_fixture.dylib",
        _ => "libpluginhost_malformed_abi_fixture.so",
    })
}

fn fixture_mode(mode: Option<&str>) -> Option<String> {
    let previous = std::env::var("EZF_PLUGIN_FIXTURE_MODE").ok();
    unsafe {
        match mode {
            Some(value) => std::env::set_var("EZF_PLUGIN_FIXTURE_MODE", value),
            None => std::env::remove_var("EZF_PLUGIN_FIXTURE_MODE"),
        }
    }
    previous
}

fn restore_fixture_mode(previous: Option<String>) {
    unsafe {
        match previous {
            Some(value) => std::env::set_var("EZF_PLUGIN_FIXTURE_MODE", value),
            None => std::env::remove_var("EZF_PLUGIN_FIXTURE_MODE"),
        }
    }
}

#[test]
fn malformed_abi_dynamic_libraries_are_rejected_without_registration() {
    let _test_guard = lock_test();
    let library = build_malformed_fixture();
    let cases = [
        "constructor_null",
        "bad_instance_version",
        "bad_instance_size",
        "null_context",
        "null_vtable",
        "bad_vtable_version",
        "bad_vtable_size",
    ];

    for case in cases {
        let previous = {
            let old = std::env::var("EZF_MALFORMED_ABI_KIND").ok();
            unsafe {
                std::env::set_var("EZF_MALFORMED_ABI_KIND", case);
            }
            old
        };

        let error = Factory::load_plugin_file(&library).expect_err(
            "malformed ABI library must be rejected"
        );
        assert!(
            matches!(error, PluginManagerError::PluginAbiIncompatible { .. }),
            "unexpected error for malformed ABI case {case}: {error}"
        );
        assert_eq!(Factory::plugin_count(), 0, "malformed case {case} must not register a plugin");

        unsafe {
            match previous {
                Some(value) => std::env::set_var("EZF_MALFORMED_ABI_KIND", value),
                None => std::env::remove_var("EZF_MALFORMED_ABI_KIND"),
            }
        }
    }

    let _ = fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn plugin_panic_is_isolated_and_the_runtime_can_continue_serving_requests() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let previous = fixture_mode(Some("panic"));
    let id = Factory::load_plugin_file(&library).expect("panic fixture must load");
    restore_fixture_mode(previous);

    let mut request = ping_request();
    request.endpoint_id = "panic".to_owned();
    request.raw_path = "/pluginhost-integration/panic".to_owned();

    let error = Factory::handle_endpoint(request).expect_err(
        "intentional plugin panic must cross the ABI as an error"
    );
    assert_eq!(error.status, 500);
    assert_eq!(error.code, "plugin_abi_error");

    let health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "panicked plugin health must be available"
    );
    assert_eq!(health.state, PluginHealthState::Degraded);
    assert_eq!(health.failed_calls, 1);

    let response = Factory::handle_endpoint(ping_request()).expect(
        "a subsequent non-panicking call must still succeed"
    );
    assert_eq!(response.status, 200);

    let health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "healthy follow-up health must be available"
    );
    assert_eq!(health.state, PluginHealthState::Ready);
    assert_eq!(health.failed_calls, 1);

    cleanup_plugin(id);
    let _ = fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn on_unload_timeout_keeps_the_plugin_registered_until_callback_finishes() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let config = PluginRuntimeConfig {
        call_timeout: std::time::Duration::from_secs(2),
        unload_timeout: std::time::Duration::from_secs(2),
        lifecycle_timeout: std::time::Duration::from_millis(25),
        worker_threads: 2,
        max_concurrent_calls: 2,
        queue_capacity: 2,
    };
    let previous = fixture_mode(Some("slow_on_unload"));
    let id = Factory::load_plugin_file_with_config(&library, config).expect(
        "slow on_unload fixture must load"
    );
    restore_fixture_mode(previous);

    let error = Factory::unload_plugin(id).expect_err(
        "on_unload timeout must keep the runtime registered instead of forcing unload"
    );
    assert!(matches!(error, PluginManagerError::PluginUnloadFailed { .. }));
    assert_eq!(Factory::plugin_count(), 1);
    let health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "plugin health must remain visible during timed-out on_unload"
    );
    assert_eq!(health.state, PluginHealthState::Stopping);
    assert!(!library.parent().unwrap().join("destroy.marker").is_file());

    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(library.parent().unwrap().join("unload-cancelled.marker").is_file());

    Factory::unload_plugin(id).expect(
        "retry must observe the finished on_unload and proceed to destroy"
    );
    assert_eq!(Factory::plugin_count(), 0);
    assert!(library.parent().unwrap().join("destroy.marker").is_file());

    let lifecycle = std::fs
        ::read_to_string(library.parent().unwrap().join("lifecycle.order"))
        .expect("fixture must record lifecycle events");
    let on_unload_count = lifecycle
        .lines()
        .filter(|line| *line == "on_unload")
        .count();
    let on_destroy_count = lifecycle
        .lines()
        .filter(|line| *line == "on_destroy")
        .count();
    assert_eq!(
        on_unload_count,
        1,
        "successful timed-out on_unload must not be invoked twice: {lifecycle}"
    );
    assert_eq!(on_destroy_count, 1, "destroy must run exactly once: {lifecycle}");
    let _ = std::fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn destroy_timeout_keeps_the_plugin_registered_until_callback_finishes() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let config = PluginRuntimeConfig {
        call_timeout: std::time::Duration::from_secs(2),
        unload_timeout: std::time::Duration::from_secs(2),
        lifecycle_timeout: std::time::Duration::from_millis(25),
        worker_threads: 2,
        max_concurrent_calls: 2,
        queue_capacity: 2,
    };
    let previous = fixture_mode(Some("slow_destroy"));
    let id = Factory::load_plugin_file_with_config(&library, config).expect(
        "slow destroy fixture must load"
    );
    restore_fixture_mode(previous);

    let error = Factory::unload_plugin(id).expect_err(
        "destroy timeout must keep the runtime registered instead of risking a use-after-free"
    );
    assert!(matches!(error, PluginManagerError::PluginUnloadFailed { .. }));
    assert_eq!(Factory::plugin_count(), 1);
    let health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "plugin health must remain visible during timed-out destroy"
    );
    assert_eq!(health.state, PluginHealthState::Stopping);

    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(library.parent().unwrap().join("destroy-cancelled.marker").is_file());

    Factory::unload_plugin(id).expect(
        "retry must observe the finished destroy without invoking destroy twice"
    );
    assert_eq!(Factory::plugin_count(), 0);
    let _ = std::fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn on_unload_failure_keeps_the_plugin_registered_and_allows_retry() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let previous = fixture_mode(Some("on_unload_failure"));
    let id = Factory::load_plugin_file(&library).expect("unload-failure fixture must load");
    restore_fixture_mode(previous);

    let error = Factory::unload_plugin(id).expect_err("first unload must fail deliberately");
    assert!(matches!(error, PluginManagerError::PluginUnloadFailed { .. }));
    assert_eq!(Factory::plugin_count(), 1);

    let health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "failed unload health must be available"
    );
    assert_eq!(health.state, PluginHealthState::Failed);
    assert!(library.parent().unwrap().join("unload.marker").is_file());

    Factory::unload_plugin(id).expect("second unload must succeed after the one-shot failure");
    assert_eq!(Factory::plugin_count(), 0);
    assert!(library.parent().unwrap().join("destroy.marker").is_file());
    let _ = fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn destroy_failure_keeps_the_instance_and_library_loaded_until_retry_succeeds() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let previous = fixture_mode(Some("destroy_failure"));
    let id = Factory::load_plugin_file(&library).expect("destroy-failure fixture must load");
    restore_fixture_mode(previous);

    let error = Factory::unload_plugin(id).expect_err("first destroy must fail deliberately");
    assert!(matches!(error, PluginManagerError::PluginUnloadFailed { .. }));
    assert_eq!(Factory::plugin_count(), 1);

    let health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "failed destroy health must be available"
    );
    assert_eq!(health.state, PluginHealthState::Failed);
    assert!(
        health.last_error
            .as_deref()
            .is_some_and(|message| message.contains("destroy returned status"))
    );

    Factory::unload_plugin(id).expect("second destroy must succeed after the one-shot failure");
    assert_eq!(Factory::plugin_count(), 0);

    let lifecycle = fs
        ::read_to_string(library.parent().unwrap().join("lifecycle.order"))
        .expect("fixture must record lifecycle events");
    let on_unload_count = lifecycle
        .lines()
        .filter(|line| *line == "on_unload")
        .count();
    let on_destroy_count = lifecycle
        .lines()
        .filter(|line| *line == "on_destroy")
        .count();
    assert_eq!(
        on_unload_count,
        1,
        "successful on_unload must not be repeated when destroy is retried: {lifecycle}"
    );
    assert_eq!(on_destroy_count, 2, "one failed destroy plus one retry are expected: {lifecycle}");
    let _ = fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn catalog_registration_failure_rolls_back_the_loaded_library_and_instance() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let first = Factory::load_plugin_file(&library).expect("base fixture must load");

    let conflict_dir = std::env
        ::temp_dir()
        .join(format!("ezf-pluginhost-catalog-rollback-{}", std::process::id()));
    let _ = fs::remove_dir_all(&conflict_dir);
    fs::create_dir_all(&conflict_dir).expect("catalog rollback directory must be creatable");
    let conflict_library = conflict_dir.join(match std::env::consts::DLL_EXTENSION {
        "dll" => "conflict.dll",
        "dylib" => "conflict.dylib",
        _ => "conflict.so",
    });
    fs::copy(&library, &conflict_library).expect("conflict fixture must be copied");

    let previous = fixture_mode(Some("catalog_conflict"));
    let error = Factory::load_plugin_file(&conflict_library).expect_err(
        "catalog route conflict must reject registration"
    );
    restore_fixture_mode(previous);

    assert!(matches!(error, PluginManagerError::InvalidCatalog(_)));
    assert_eq!(Factory::plugin_count(), 1, "rollback must preserve the already loaded plugin");
    assert!(conflict_dir.join("load-conflict.marker").is_file());
    assert!(conflict_dir.join("unload.marker").is_file());
    assert!(conflict_dir.join("destroy.marker").is_file());

    cleanup_plugin(first);
    let _ = fs::remove_dir_all(library.parent().unwrap());
    let _ = fs::remove_dir_all(conflict_dir);
}

#[test]
fn required_and_optional_loading_policy_covers_success_failure_and_missing_plugin_cases() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let root = std::env
        ::temp_dir()
        .join(format!("ezf-pluginhost-policy-matrix-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("policy matrix root must be creatable");

    let good = root.join("PluginHostIntegration");
    let good_file = good.with_extension(std::env::consts::DLL_EXTENSION);
    fs::copy(&library, &good_file).expect("good plugin must be copied");

    let report = Factory::load_plugins_with_policy(
        root.to_str().unwrap(),
        PluginLoadPolicy::required_all()
    ).expect("required valid plugin must load");
    assert_eq!(report.loaded, vec!["PluginHostIntegration"]);
    assert!(report.failures.is_empty());
    let id = Factory::loaded_plugin_ids().into_iter().next().expect("plugin id must exist");
    cleanup_plugin(id);

    let optional_broken = root.join(format!("optional_broken.{}", std::env::consts::DLL_EXTENSION));
    fs::write(&optional_broken, b"not a dynamic library").expect(
        "optional broken file must be written"
    );
    let report = Factory::load_plugins_with_policy(
        root.to_str().unwrap(),
        PluginLoadPolicy::optional_all()
    ).expect("optional broken plugin must not abort loading");
    assert_eq!(report.loaded, vec!["PluginHostIntegration"]);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].requirement, PluginRequirement::Optional);
    let id = Factory::loaded_plugin_ids().into_iter().next().expect("plugin id must exist");
    cleanup_plugin(id);
    fs::remove_file(&good_file).expect("good fixture must be removable");

    let required_broken = root.join(format!("requiredbroken.{}", std::env::consts::DLL_EXTENSION));
    fs::write(&required_broken, b"not a dynamic library").expect(
        "required broken file must be written"
    );
    let error = Factory::load_plugins_with_policy(
        root.to_str().unwrap(),
        PluginLoadPolicy::optional_all().require("requiredbroken")
    ).expect_err("required broken plugin must abort loading");
    match error {
        PluginManagerError::PluginLoadPolicyViolation { report } => {
            assert!(report.has_required_failures());
            assert_eq!(report.required_failures().len(), 1);
            assert_eq!(
                report.required_failures()[0].plugin_name.as_deref(),
                Some("requiredbroken")
            );
        }
        other => panic!("unexpected required broken policy error: {other}"),
    }
    fs::remove_file(&required_broken).expect("required broken fixture must be removable");

    let empty = root.join("empty");
    fs::create_dir_all(&empty).expect("empty policy directory must be creatable");
    let error = Factory::load_plugins_with_policy(
        empty.to_str().unwrap(),
        PluginLoadPolicy::optional_all().require("MissingRequiredPlugin")
    ).expect_err("missing required plugin must abort loading");
    match error {
        PluginManagerError::PluginLoadPolicyViolation { report } => {
            assert!(report.has_required_failures());
            assert_eq!(
                report.required_failures()[0].plugin_name.as_deref(),
                Some("missingrequiredplugin")
            );
        }
        other => panic!("unexpected missing required policy error: {other}"),
    }

    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_dir_all(library.parent().unwrap());
}

static DATABASE_CAPABILITY_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(
    0
);

unsafe extern "C" fn test_database_execute(
    request: PluginContracts::PluginAbi::AbiSlice,
    cancellation: PluginContracts::PluginAbi::PluginCancellationV2,
    response_out: *mut PluginContracts::PluginAbi::AbiBuffer
) -> i32 {
    if response_out.is_null() {
        return PluginContracts::PluginAbi::PLUGIN_STATUS_INVALID_ARGUMENT;
    }

    let request = (unsafe { request.to_vec() }).and_then(|bytes|
        serde_json::from_slice::<DatabaseContracts::DatabaseRequest>(&bytes).ok()
    );
    let Some(request) = request else {
        return PluginContracts::PluginAbi::PLUGIN_STATUS_INVALID_ARGUMENT;
    };

    DATABASE_CAPABILITY_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    let is_cancellable_probe = match &request {
        DatabaseContracts::DatabaseRequest::Execute(execute) => execute.statement == "CANCEL_ME",
        DatabaseContracts::DatabaseRequest::Query(query) => query.statement == "CANCEL_ME",
    };
    if is_cancellable_probe {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            let is_cancelled = unsafe {
                let c = cancellation.is_cancelled();
                c
            };
            match is_cancelled {
                true => {
                    return PluginContracts::PluginAbi::PLUGIN_STATUS_CANCELLED;
                }
                false => (),
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        return PluginContracts::PluginAbi::PLUGIN_STATUS_OK;
    }

    let result: Result<DatabaseContracts::DatabaseResponse, DatabaseContracts::DatabaseError> = Ok(
        DatabaseContracts::DatabaseResponse::Execute(DatabaseContracts::ExecutionResult {
            rows_affected: 42,
        })
    );
    let bytes = serde_json::to_vec(&result).expect("test database response must serialize");
    unsafe {
        *response_out = PluginContracts::PluginAbi::AbiBuffer::from_vec(bytes);
    }
    PluginContracts::PluginAbi::PLUGIN_STATUS_OK
}

unsafe extern "C" fn test_is_cancelled(context: *const std::ffi::c_void) -> bool {
    if context.is_null() {
        return true;
    }
    unsafe {
        (*(context as *const std::sync::atomic::AtomicBool)).load(
            std::sync::atomic::Ordering::Acquire
        )
    }
}

unsafe extern "C" fn test_database_free(buffer: PluginContracts::PluginAbi::AbiBuffer) {
    unsafe {
        buffer.free();
    }
}

#[test]
fn database_capability_crosses_the_real_plugin_abi_boundary() {
    let _test_guard = lock_test();
    let library = build_fixture();

    DATABASE_CAPABILITY_CALLS.store(0, std::sync::atomic::Ordering::SeqCst);
    Factory::register_database_executor(test_database_execute, test_database_free).expect(
        "test database capability must be registered once"
    );

    let previous = fixture_mode(Some("database"));
    let id = Factory::load_plugin_file(&library).expect("database bridge fixture must load");
    restore_fixture_mode(previous);

    let request = DatabaseContracts::DatabaseRequest::Execute(DatabaseContracts::ExecuteRequest {
        statement: "UPDATE test_table SET value = 1".to_owned(),
        parameters: Vec::new(),
    });
    let response = Factory::with_database_plugin(|plugin|
        plugin.execute_database(
            request.clone(),
            &PluginContracts::ApiContract::PluginCancellation::uncancelled()
        )
    )
        .expect("exactly one database plugin must be selected")
        .expect("plugin database bridge must execute through host capability");

    assert_eq!(DATABASE_CAPABILITY_CALLS.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        response,
        DatabaseContracts::DatabaseResponse::Execute(DatabaseContracts::ExecutionResult {
            rows_affected: 42,
        })
    );

    // The raw DataSource dispatch path must preserve the serialized Result envelope while still
    // using the cached unique-DataSource selection and the normal ABI/cancellation boundary.
    let request_bytes = serde_json::to_vec(&request).expect("database request must serialize");
    let raw_response = Factory::execute_database_raw(
        request_bytes,
        PluginContracts::ApiContract::PluginCancellation::uncancelled().as_abi()
    ).expect("raw DataSource dispatch must return its ABI payload");
    let decoded: Result<
        DatabaseContracts::DatabaseResponse,
        DatabaseContracts::DatabaseError
    > = serde_json::from_slice(&raw_response).expect("raw DataSource response must remain JSON");
    assert_eq!(decoded.expect("raw DataSource result must be successful"), response);
    assert_eq!(DATABASE_CAPABILITY_CALLS.load(std::sync::atomic::Ordering::SeqCst), 2);

    cleanup_plugin(id);
    let _ = fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn database_capability_propagates_cooperative_cancellation_across_the_plugin_boundary() {
    let _test_guard = lock_test();
    let library = build_fixture();

    DATABASE_CAPABILITY_CALLS.store(0, std::sync::atomic::Ordering::SeqCst);
    let _ = Factory::register_database_executor(test_database_execute, test_database_free);

    let previous = fixture_mode(Some("database"));
    let id = Factory::load_plugin_file(&library).expect("database bridge fixture must load");
    restore_fixture_mode(previous);

    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let setter = std::sync::Arc::clone(&cancelled);
    let setter_thread = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(50));
        setter.store(true, std::sync::atomic::Ordering::Release);
    });

    let cancellation = PluginContracts::ApiContract::PluginCancellation::from_abi(
        PluginContracts::PluginAbi::PluginCancellationV2 {
            context: std::sync::Arc::as_ptr(&cancelled).cast(),
            is_cancelled: test_is_cancelled,
        }
    );
    let request = DatabaseContracts::DatabaseRequest::Execute(DatabaseContracts::ExecuteRequest {
        statement: "CANCEL_ME".to_owned(),
        parameters: Vec::new(),
    });

    let error = Factory::with_database_plugin(|plugin|
        plugin.execute_database(request, &cancellation)
    )
        .expect("exactly one database plugin must be selected")
        .expect_err("cooperative cancellation must reach the host database capability");
    setter_thread.join().expect("cancellation setter thread must finish");

    assert!(error.to_string().contains("host database capability returned ABI status -6"));
    assert_eq!(DATABASE_CAPABILITY_CALLS.load(std::sync::atomic::Ordering::SeqCst), 1);

    cleanup_plugin(id);
    let _ = fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn plugin_concurrency_limit_rejects_excess_calls_and_reports_overload() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let config = PluginRuntimeConfig {
        // The fixture deliberately sleeps for 200 ms. Keep the execution timeout above that
        // duration so this test isolates the admission/concurrency limit rather than timing out
        // the admitted call itself.
        call_timeout: std::time::Duration::from_secs(1),
        unload_timeout: std::time::Duration::from_secs(2),
        lifecycle_timeout: std::time::Duration::from_secs(2),
        worker_threads: 1,
        max_concurrent_calls: 1,
        queue_capacity: 1,
    };
    let id = Factory::load_plugin_file_with_config(&library, config).expect(
        "fixture library must load with a single-call concurrency limit"
    );

    let marker = library.parent().unwrap().join("slow-entered.marker");
    let _ = fs::remove_file(&marker);
    let first = std::thread::spawn(|| {
        let mut request = ping_request();
        request.endpoint_id = "slow".to_owned();
        request.raw_path = "/pluginhost-integration/slow".to_owned();
        Factory::handle_endpoint(request).expect("first slow call must succeed")
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !marker.is_file() {
        assert!(std::time::Instant::now() < deadline, "first slow call did not signal entry");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    let mut second_request = ping_request();
    second_request.endpoint_id = "slow".to_owned();
    second_request.raw_path = "/pluginhost-integration/slow".to_owned();
    let error = Factory::handle_endpoint(second_request).expect_err(
        "second call must be rejected while the concurrency limit is full"
    );
    assert_eq!(error.status, 503);
    assert_eq!(error.code, "plugin_overloaded");

    let health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "overloaded plugin health must be available"
    );
    assert_eq!(health.active_calls, 1);
    assert_eq!(health.rejected_calls, 1);

    let first_response = first.join().expect("first slow call worker must finish");
    assert_eq!(first_response.status, 200);
    cleanup_plugin(id);
    let _ = fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn full_worker_queue_rejects_excess_calls_as_overload() {
    let _test_guard = lock_test();
    let _environment = EnvironmentVariableGuard::set("EZF_PLUGIN_FIXTURE_MODE", "slow_long");
    let library = build_fixture();
    let config = PluginRuntimeConfig {
        call_timeout: std::time::Duration::from_secs(8),
        unload_timeout: std::time::Duration::from_secs(3),
        lifecycle_timeout: std::time::Duration::from_secs(3),
        worker_threads: 1,
        max_concurrent_calls: 3,
        queue_capacity: 1,
    };
    let id = Factory::load_plugin_file_with_config(&library, config).expect(
        "fixture library must load with independent worker, admission, and queue limits"
    );

    let marker = library.parent().unwrap().join("slow-entered.marker");
    let _ = fs::remove_file(&marker);
    let slow_call = || {
        let mut request = ping_request();
        request.endpoint_id = "slow".to_owned();
        request.raw_path = "/pluginhost-integration/slow".to_owned();
        Factory::handle_endpoint(request)
    };

    let first = std::thread::spawn(slow_call);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !marker.is_file() {
        assert!(std::time::Instant::now() < deadline, "first slow call did not signal entry");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    // One worker is busy. Admit one queued request, leaving capacity in the admission gate so
    // the third request specifically exercises the bounded queue's overload behavior.
    let second = std::thread::spawn(slow_call);
    let admission_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    loop {
        let health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
            "plugin health must remain available while queueing"
        );
        if health.active_calls >= 2 {
            break;
        }
        assert!(std::time::Instant::now() < admission_deadline, "second call was not admitted");
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    // The fixture holds the worker for 2.5s, so the admitted second call can settle into the
    // one-slot queue before the next request is attempted, even under a loaded test runner.
    std::thread::sleep(std::time::Duration::from_millis(50));

    let error = slow_call().expect_err("a full queue must reject the third call");
    assert_eq!(error.status, 503);
    assert_eq!(error.code, "plugin_overloaded");

    assert_eq!(first.join().expect("first request thread must finish").unwrap().status, 200);
    assert_eq!(second.join().expect("second request thread must finish").unwrap().status, 200);
    let health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "plugin health must be available after queue drain"
    );
    assert_eq!(health.rejected_calls, 1, "queue overflow must be counted as a rejection");
    assert_eq!(
        health.failed_calls,
        0,
        "queue overflow must not count as an internal plugin failure"
    );
    assert_eq!(health.active_calls, 0);

    cleanup_plugin(id);
    let _ = std::fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn completed_plugin_call_releases_its_concurrency_slot_before_result_is_observed() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let config = PluginRuntimeConfig {
        call_timeout: std::time::Duration::from_secs(1),
        unload_timeout: std::time::Duration::from_secs(2),
        lifecycle_timeout: std::time::Duration::from_secs(2),
        worker_threads: 1,
        max_concurrent_calls: 1,
        queue_capacity: 1,
    };
    let id = Factory::load_plugin_file_with_config(&library, config).expect(
        "fixture library must load with a single-call concurrency limit"
    );

    // Run a sustained sequence of short calls. The next call must never observe the previous
    // call as still occupying the single admission slot after its result was returned.
    for iteration in 0..1_000 {
        let response = Factory::handle_endpoint(ping_request()).unwrap_or_else(|error| {
            panic!(
                "iteration {iteration}: completed call left the concurrency slot occupied: {error}"
            )
        });
        assert_eq!(response.status, 200);
    }

    let health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "plugin health must remain available"
    );
    assert_eq!(health.rejected_calls, 0);
    assert_eq!(health.active_calls, 0);

    Factory::unload_plugin(id).expect("single-call fixture must unload cleanly");
    let _ = std::fs::remove_dir_all(library.parent().unwrap());
}

#[test]
fn concurrent_load_and_unload_for_the_same_library_are_serialized_and_reload_remains_possible() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let config = PluginRuntimeConfig {
        call_timeout: std::time::Duration::from_secs(5),
        unload_timeout: std::time::Duration::from_secs(5),
        lifecycle_timeout: std::time::Duration::from_secs(2),
        worker_threads: 2,
        max_concurrent_calls: 2,
        queue_capacity: 2,
    };
    let id = Factory::load_plugin_file_with_config(&library, config).expect(
        "fixture library must load before concurrent lifecycle test"
    );

    let plugin_dir = library.parent().unwrap();
    // Reuse the established `slow` route. The hold mode is selected through the query string so
    // this lifecycle test does not depend on registering a separate fixture endpoint in the
    // runtime catalog.
    let entered_marker = plugin_dir.join("slow-entered.marker");
    let release_marker = plugin_dir.join("slow-release.marker");
    let _ = fs::remove_file(&entered_marker);
    let _ = fs::remove_file(&release_marker);
    let worker = std::thread::spawn(|| {
        let mut request = ping_request();
        request.endpoint_id = "slow".to_owned();
        request.raw_path = "/pluginhost-integration/slow".to_owned();
        request.raw_query = Some("hold=true".to_owned());
        Factory::handle_endpoint(request).expect_err(
            "held endpoint must observe cooperative unload cancellation after release"
        )
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !entered_marker.is_file() {
        if worker.is_finished() {
            match worker.join() {
                Ok(endpoint_error) =>
                    panic!("held endpoint returned before signaling entry: {:?}", endpoint_error),
                Err(_) => panic!("held endpoint worker panicked before signaling entry"),
            }
        }
        if std::time::Instant::now() >= deadline {
            // Do not leave a fixture callback blocked if its entry marker was not observed.
            let _ = fs::write(&release_marker, b"release");
            match worker.join() {
                Ok(endpoint_error) =>
                    panic!(
                        "held endpoint did not signal entry before the deadline; worker returned: {:?}",
                        endpoint_error
                    ),
                Err(_) => panic!("held endpoint did not signal entry and worker panicked"),
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    let unload_id = id;
    let unload_thread = std::thread::spawn(move || Factory::unload_plugin(unload_id));

    // The fixture deliberately keeps the in-flight call alive after cancellation. This gives the
    // test a deterministic synchronization point: unload cannot finish until release_marker exists.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut observed_stopping = false;
    while std::time::Instant::now() < deadline {
        if let Some(health) = Factory::plugin_health_by_name("PluginHostIntegration") {
            if health.state == PluginHealthState::Stopping {
                observed_stopping = true;
                break;
            }
        }
        std::thread::yield_now();
    }

    // Try the concurrent load while the fixture is guaranteed to remain inside the unload path.
    let concurrent_load = if observed_stopping {
        Some(Factory::load_plugin_file_with_config(&library, config))
    } else {
        None
    };
    // Always release the fixture before assertions or joins, including the failure path, so a
    // failed assertion cannot strand a plugin worker or make the test process hang.
    fs::write(&release_marker, b"release").expect("held endpoint must be released");

    let unload_result = unload_thread.join().expect("unload worker must finish");
    let cancellation_error = worker.join().expect("in-flight endpoint worker must finish");

    assert!(
        observed_stopping,
        "unload must expose the transient stopping state before the concurrent load attempt"
    );
    assert!(
        matches!(concurrent_load, Some(Err(PluginManagerError::PluginLoadInProgress { .. }))),
        "concurrent load must be rejected by the per-library lifecycle guard while unload is stopping"
    );
    unload_result.expect("unload must succeed after the in-flight call drains");
    assert_eq!(cancellation_error.code, "plugin_cancelled");
    assert_eq!(Factory::plugin_count(), 0);

    let reloaded_id = Factory::load_plugin_file_with_config(&library, config).expect(
        "library must be reloadable after serialized unload"
    );
    cleanup_plugin(reloaded_id);
    let _ = fs::remove_dir_all(plugin_dir);
}
