use PluginContracts::ApiContract::{ PluginEndpointRequest, PluginHttpMethod, PluginPrincipal };
use PluginHost::Factory::{ self, PluginHealthState, PluginRuntimeConfig };
use std::{
    fs,
    path::{ Path, PathBuf },
    process::Command,
    sync::{ atomic::{ AtomicUsize, Ordering }, Mutex, MutexGuard },
    thread,
    time::{ Duration, Instant },
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
        .join(format!("ezf-pluginhost-p2-{}-{}", std::process::id(), build_id));
    let target_dir = temp_dir.join("target");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&target_dir).expect("P2 fixture target directory must be creatable");

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
        .expect("cargo must be available to build the P2 dynamic test plugin");
    assert!(status.success(), "P2 fixture failed to build");

    target_dir.join("debug").join(fixture_package_target_name())
}

fn existing_plugin_manifest(package: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("Plugins")
        .join(package)
        .join("Cargo.toml")
}

fn existing_plugin_binary_name(package: &str) -> String {
    #[cfg(target_os = "windows")]
    {
        return format!("{package}.dll");
    }
    #[cfg(target_os = "macos")]
    {
        return format!("lib{package}.dylib");
    }
    #[cfg(target_os = "linux")]
    {
        return format!("lib{package}.so");
    }
}

fn build_existing_plugin(package: &str) -> PathBuf {
    let manifest = existing_plugin_manifest(package);
    assert!(
        manifest.is_file(),
        "production plugin manifest for {package} must exist at {}",
        manifest.display()
    );

    static BUILD_COUNTER: AtomicUsize = AtomicUsize::new(10_000);
    let build_id = BUILD_COUNTER.fetch_add(1, Ordering::Relaxed);
    let temp_dir = std::env
        ::temp_dir()
        .join(
            format!(
                "ezf-pluginhost-p2-existing-{}-{}-{}",
                std::process::id(),
                build_id,
                package.to_ascii_lowercase()
            )
        );
    let target_dir = temp_dir.join("target");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&target_dir).expect(
        "existing production plugin target directory must be creatable"
    );

    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = Command::new(cargo)
        .arg("build")
        .arg("--manifest-path")
        .arg(&manifest)
        .arg("--target-dir")
        .arg(&target_dir)
        .arg("--quiet")
        .status()
        .unwrap_or_else(|error| panic!("cargo must be available to build {package}: {error}"));
    assert!(status.success(), "production plugin {package} failed to build");

    target_dir.join("debug").join(existing_plugin_binary_name(package))
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
            user_id: "p2-test".to_owned(),
            username: "p2-test".to_owned(),
            given_name: "P2".to_owned(),
            family_name: "Test".to_owned(),
            groups: Vec::new(),
            session_id: "p2-session".to_owned(),
        },
    }
}

fn p2_usize(name: &str, default: usize) -> usize {
    std::env
        ::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn p2_duration_secs(name: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env
            ::var(name)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(default)
    )
}

static TEST_LOCK: Mutex<()> = Mutex::new(());

struct TestGuard {
    _lock: MutexGuard<'static, ()>,
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        for _ in 0..5 {
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
            thread::sleep(Duration::from_millis(10));
        }
    }
}

fn lock_test() -> TestGuard {
    TestGuard {
        _lock: TEST_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner()),
    }
}

fn unload_and_assert_empty(id: i64) {
    Factory::unload_plugin(id).expect("P2 fixture plugin must unload cleanly");
    assert_eq!(Factory::plugin_count(), 0, "no plugin may remain after unload");
}

#[test]
#[ignore = "P2 production stress gate; run explicitly with --ignored"]
fn p2_long_running_plugin_stress() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let workers = p2_usize("EZF_P2_STRESS_WORKERS", 8).min(32);
    let config = PluginRuntimeConfig {
        call_timeout: Duration::from_secs(5),
        unload_timeout: Duration::from_secs(5),
        max_concurrent_calls: workers.max(1),
    };
    let id = Factory::load_plugin_file_with_config(&library, config).expect(
        "P2 stress fixture must load"
    );
    let duration = p2_duration_secs("EZF_P2_STRESS_SECS", 10);
    let deadline = Instant::now() + duration;
    let total_successes = std::sync::Arc::new(AtomicUsize::new(0));
    let total_failures = std::sync::Arc::new(AtomicUsize::new(0));

    let handles = (0..workers)
        .map(|_| {
            let successes = std::sync::Arc::clone(&total_successes);
            let failures = std::sync::Arc::clone(&total_failures);
            thread::spawn(move || {
                while Instant::now() < deadline {
                    match Factory::handle_endpoint("PluginHostIntegration", ping_request()) {
                        Ok(response) if response.status == 200 => {
                            successes.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {
                            failures.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    thread::yield_now();
                }
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().expect("P2 stress worker must finish without panic");
    }

    let successes = total_successes.load(Ordering::Relaxed);
    let failures = total_failures.load(Ordering::Relaxed);
    assert!(successes > 0, "long-running stress must execute at least one successful call");
    assert_eq!(failures, 0, "long-running stress must not produce endpoint failures");

    let health = Factory::plugin_health_by_name("PluginHostIntegration").expect(
        "stress plugin health must remain available"
    );
    assert_eq!(health.state, PluginHealthState::Ready);
    assert_eq!(health.failed_calls, 0);
    assert_eq!(health.rejected_calls, 0);
    assert_eq!(health.active_calls, 0);
    assert!((health.total_calls as usize) >= successes);

    unload_and_assert_empty(id);
    cleanup_fixture(&library);
}

#[test]
#[ignore = "P2 production soak gate; run explicitly with --ignored"]
fn p2_repeated_load_unload_soak() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let cycles = p2_usize("EZF_P2_SOAK_CYCLES", 100);
    for cycle in 0..cycles {
        let id = Factory::load_plugin_file(&library).unwrap_or_else(|error|
            panic!("cycle {cycle}: plugin must load: {error}")
        );
        assert!(id > 0, "cycle {cycle}: plugin id must be valid");
        assert_eq!(Factory::plugin_count(), 1, "cycle {cycle}: exactly one plugin must be loaded");

        let response = Factory::handle_endpoint(
            "PluginHostIntegration",
            ping_request()
        ).unwrap_or_else(|error| panic!("cycle {cycle}: endpoint call failed: {error}"));
        assert_eq!(response.status, 200, "cycle {cycle}: endpoint must return HTTP 200");

        Factory::unload_plugin(id).unwrap_or_else(|error|
            panic!("cycle {cycle}: plugin unload failed: {error}")
        );
        assert_eq!(Factory::plugin_count(), 0, "cycle {cycle}: registry must be empty");
    }

    assert_eq!(Factory::loaded_plugin_ids(), Vec::<i64>::new());
    cleanup_fixture(&library);
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "P2 Linux resource leak gate; run explicitly with --ignored"]
fn p2_resource_handle_and_thread_leak_check() {
    let _test_guard = lock_test();
    let library = build_fixture();
    let warmup_id = Factory::load_plugin_file(&library).expect("leak-check warmup load must work");
    Factory::handle_endpoint("PluginHostIntegration", ping_request()).expect(
        "leak-check warmup call must work"
    );
    Factory::unload_plugin(warmup_id).expect("leak-check warmup unload must work");

    let baseline = resource_snapshot(&library);
    let cycles = p2_usize("EZF_P2_LEAK_CYCLES", 75);

    for cycle in 0..cycles {
        let id = Factory::load_plugin_file(&library).unwrap_or_else(|error|
            panic!("cycle {cycle}: plugin load failed: {error}")
        );
        Factory::handle_endpoint("PluginHostIntegration", ping_request()).unwrap_or_else(|error|
            panic!("cycle {cycle}: endpoint call failed: {error}")
        );
        Factory::unload_plugin(id).unwrap_or_else(|error|
            panic!("cycle {cycle}: plugin unload failed: {error}")
        );
        assert_eq!(Factory::plugin_count(), 0, "cycle {cycle}: plugin registry must be empty");

        thread::sleep(Duration::from_millis(2));
        let current = resource_snapshot(&library);
        assert_eq!(current.0, baseline.0, "cycle {cycle}: file-descriptor count leaked");
        assert_eq!(current.1, baseline.1, "cycle {cycle}: thread count leaked");
        assert_eq!(current.2, 0, "cycle {cycle}: dynamic library remains mapped after unload");
    }

    thread::sleep(Duration::from_millis(25));
    let final_snapshot = resource_snapshot(&library);
    assert_eq!(final_snapshot.0, baseline.0, "final file-descriptor count must return to baseline");
    assert_eq!(final_snapshot.1, baseline.1, "final thread count must return to baseline");
    assert_eq!(final_snapshot.2, 0, "final dynamic-library mapping count must be zero");

    cleanup_fixture(&library);
}

#[test]
#[ignore = "P2 multi-plugin stress gate; run explicitly with --ignored"]
fn p2_multi_plugin_load_and_call_stress() {
    let _test_guard = lock_test();
    let requested_plugins = p2_usize("EZF_P2_MULTI_PLUGIN_COUNT", 3).clamp(2, 3);
    let workers_per_plugin = p2_usize("EZF_P2_MULTI_PLUGIN_WORKERS", 2).min(16);
    let duration = p2_duration_secs("EZF_P2_MULTI_PLUGIN_SECS", 5);

    // Use plugins that are actually part of the backend instead of synthetic runtime names.
    // MolgenDB is intentionally excluded: loading it initializes database/Tokio resources and
    // would turn this host stress gate into an environment-dependent database integration test.
    let available_plugins = [
        ("PluginHostIntegration", None),
        ("VariantFilter", Some("VariantFilter")),
        ("ProcessTracker", Some("ProcessTracker")),
    ];
    let selected = &available_plugins[..requested_plugins];
    let names = selected
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .collect::<Vec<_>>();
    let libraries = selected
        .iter()
        .map(|(_, package)| {
            match package {
                None => build_fixture(),
                Some(package) => build_existing_plugin(package),
            }
        })
        .collect::<Vec<_>>();

    let config = PluginRuntimeConfig {
        call_timeout: Duration::from_secs(5),
        unload_timeout: Duration::from_secs(5),
        max_concurrent_calls: workers_per_plugin.max(1),
    };
    let load_handles = libraries
        .iter()
        .cloned()
        .map(|library| {
            let config = config;
            thread::spawn(move || Factory::load_plugin_file_with_config(&library, config))
        })
        .collect::<Vec<_>>();
    let mut ids = Vec::with_capacity(requested_plugins);
    for (name, handle) in names.iter().zip(load_handles) {
        let id = handle
            .join()
            .unwrap_or_else(|_| panic!("load worker for {name} must finish without panic"))
            .unwrap_or_else(|error| panic!("plugin {name} must load: {error}"));
        ids.push(id);
    }

    assert_eq!(Factory::plugin_count(), requested_plugins);
    let loaded_names = Factory::loaded_plugin_names();
    for name in &names {
        assert!(
            loaded_names.iter().any(|loaded| loaded == name),
            "loaded plugin set must contain {name}"
        );
    }

    let start = Instant::now();
    let integration_workers = workers_per_plugin.max(1);
    let handles = (0..integration_workers)
        .map(|_| {
            thread::spawn(move || {
                let mut successes = 0usize;
                while start.elapsed() < duration {
                    let response = Factory::handle_endpoint(
                        "PluginHostIntegration",
                        ping_request()
                    ).unwrap_or_else(|error|
                        panic!("endpoint call for PluginHostIntegration failed: {error}")
                    );
                    assert_eq!(response.status, 200);
                    successes += 1;
                    thread::yield_now();
                }
                successes
            })
        })
        .collect::<Vec<_>>();

    let mut total_successes = 0usize;
    for handle in handles {
        total_successes += handle.join().expect("multi-plugin worker must finish without panic");
    }
    assert!(total_successes > 0, "multi-plugin stress must execute endpoint traffic");

    for name in &names {
        let health = Factory::plugin_health_by_name(name).unwrap_or_else(||
            panic!("health snapshot for {name} must be available")
        );
        assert_eq!(health.state, PluginHealthState::Ready, "{name} must remain ready");
        assert_eq!(health.failed_calls, 0, "{name} must have no failed calls");
        assert_eq!(health.rejected_calls, 0, "{name} must have no rejected calls");
        assert_eq!(health.active_calls, 0, "{name} must have no active calls before unload");
        if name == "PluginHostIntegration" {
            assert!(health.total_calls > 0, "{name} must have received traffic");
        }
    }

    let unload_handles = ids
        .iter()
        .copied()
        .map(|id| thread::spawn(move || Factory::unload_plugin(id)))
        .collect::<Vec<_>>();
    for handle in unload_handles {
        handle
            .join()
            .expect("unload worker must finish without panic")
            .expect("all multi-plugin unloads must succeed");
    }
    assert_eq!(Factory::plugin_count(), 0);

    for library in libraries {
        cleanup_fixture(&library);
    }
}

#[cfg(target_os = "linux")]
fn resource_snapshot(library: &Path) -> (usize, usize, usize) {
    let fd_count = fs
        ::read_dir("/proc/self/fd")
        .expect("Linux test runner must expose /proc/self/fd")
        .count();
    let thread_count = fs
        ::read_dir("/proc/self/task")
        .expect("Linux test runner must expose /proc/self/task")
        .count();
    let mapped_library_count = fs
        ::read_to_string("/proc/self/maps")
        .expect("Linux test runner must expose /proc/self/maps")
        .lines()
        .filter(|line| line.contains(&library.to_string_lossy()))
        .count();
    (fd_count, thread_count, mapped_library_count)
}

fn cleanup_fixture(library: &Path) {
    if let Some(parent) = library.parent() {
        let _ = fs::remove_dir_all(parent);
    }
}
