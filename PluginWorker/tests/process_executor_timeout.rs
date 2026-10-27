#![cfg(unix)]

//! Verifies that an uncooperative plugin call is escalated to process termination and reaped.
//! This is a development integration test, not a production-sandbox test.

use PluginContracts::ApiContract::{PluginEndpointRequest, PluginHttpMethod, PluginPrincipal};
use PluginHost::Factory::{self, PluginHealthState, PluginRuntimeConfig};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

fn build_real_plugin() -> PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let root = std::env::temp_dir().join(format!(
        "ezf-process-timeout-fixture-{}-{}", std::process::id(), COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let target = root.join("target");
    fs::create_dir_all(&target).expect("fixture target must be creatable");
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../PluginHost/tests/pluginhost_integration_fixture/Cargo.toml");
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = Command::new(cargo)
        .arg("build").arg("--manifest-path").arg(manifest)
        .arg("--locked").arg("--target-dir").arg(&target).arg("--quiet")
        .env_remove("EZF_PLUGIN_EXECUTOR")
        .env_remove("EZF_PLUGIN_ALLOW_UNCONFINED")
        .env_remove("EZF_PLUGIN_WORKER_PATH")
        .env_remove("EZF_PLUGIN_FIXTURE_MODE")
        .status().expect("cargo must be available to build the ABI V2 fixture");
    assert!(status.success(), "the ABI V2 fixture must compile");
    #[cfg(target_os = "linux")]
    let name = "libpluginhost_integration_fixture.so";
    #[cfg(target_os = "macos")]
    let name = "libpluginhost_integration_fixture.dylib";
    target.join("debug").join(name)
}

struct EnvGuard { key: &'static str, previous: Option<std::ffi::OsString> }
impl EnvGuard {
    fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(key);
        unsafe { std::env::set_var(key, value); }
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
            user_id: "timeout-test".to_owned(),
            username: "timeout-test".to_owned(),
            given_name: "Timeout".to_owned(),
            family_name: "Test".to_owned(),
            groups: Vec::new(),
            session_id: "timeout-test-session".to_owned(),
        },
    }
}

#[test]
fn uncooperative_plugin_timeout_kills_and_reaps_worker_without_replay() {
    let worker = Path::new(env!("CARGO_BIN_EXE_ezf-plugin-worker"));
    assert!(worker.is_file(), "worker binary must exist: {}", worker.display());
    let _mode = EnvGuard::set("EZF_PLUGIN_EXECUTOR", "process");
    let _unconfined = EnvGuard::set("EZF_PLUGIN_ALLOW_UNCONFINED", "1");
    let _worker_path = EnvGuard::set("EZF_PLUGIN_WORKER_PATH", worker.as_os_str());
    let _fixture_mode = EnvGuard::set("EZF_PLUGIN_FIXTURE_MODE", "slow_ignore_cancel");
    let _grace = EnvGuard::set("EZF_PLUGIN_PROCESS_CANCEL_GRACE_MS", "100");

    let plugin = build_real_plugin();
    let mut config = PluginRuntimeConfig::default();
    config.call_timeout = Duration::from_millis(80);
    config.lifecycle_timeout = Duration::from_secs(2);
    config.unload_timeout = Duration::from_secs(2);
    let id = Factory::load_plugin_file_with_config(&plugin, config)
        .expect("fixture plugin must load through ProcessExecutor");

    let started = Instant::now();
    let result = Factory::handle_endpoint(slow_request());
    assert!(result.is_err(), "the slow endpoint must time out");
    assert!(started.elapsed() < Duration::from_secs(2), "timeout escalation must be bounded");

    let plugin_directory = plugin.parent().expect("fixture library has a parent");
    let exited_marker = plugin_directory.join("slow-exited.marker");
    std::thread::sleep(Duration::from_millis(400));
    assert!(plugin_directory.join("slow-entered.marker").is_file(), "the callback must have started in the worker");
    assert!(!exited_marker.exists(), "the worker must be killed before the uncooperative callback completes");
    let health = Factory::plugin_health_by_name("PluginHostIntegration")
        .expect("dead worker health snapshot must remain available");
    assert_eq!(health.state, PluginHealthState::Failed, "worker termination must fail runtime readiness");

    // The worker is gone and requests are never replayed or transparently restarted.
    assert!(Factory::handle_endpoint(slow_request()).is_err(), "a dead worker must fail subsequent operations");
    // Explicit unload cannot run plugin callbacks after the worker has exited. The supervisor has
    // already killed and reaped it; do not pretend this is a graceful plugin cleanup.
    assert!(Factory::unload_plugin(id).is_err(), "dead-worker cleanup must not be reported as graceful");
    let _ = fs::remove_dir_all(plugin.parent().and_then(Path::parent).and_then(Path::parent).unwrap_or_else(|| Path::new("/nonexistent")));
}
