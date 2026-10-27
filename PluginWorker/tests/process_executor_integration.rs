#![cfg(unix)]

//! End-to-end integration for Factory -> ProcessExecutor -> real worker process -> ABI V2 fixture.
//! This test is intentionally unconfined and must never be used as a sandbox acceptance test.

use PluginContracts::ApiContract::{ PluginEndpointRequest, PluginHttpMethod, PluginPrincipal };
use PluginHost::Factory::{ self, PluginRuntimeConfig };
use std::fs;
use std::path::{ Path, PathBuf };
use std::process::Command;
use std::sync::atomic::{ AtomicUsize, Ordering };

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

#[test]
fn factory_uses_process_executor_for_all_runtime_calls_and_unloads_worker() {
    let worker = Path::new(env!("CARGO_BIN_EXE_ezf-plugin-worker"));
    assert!(worker.is_file(), "Cargo-built worker binary must exist: {}", worker.display());
    let _mode = EnvGuard::set("EZF_PLUGIN_EXECUTOR", "process");
    let _unconfined = EnvGuard::set("EZF_PLUGIN_ALLOW_UNCONFINED", "1");
    let _worker_path = EnvGuard::set("EZF_PLUGIN_WORKER_PATH", worker.as_os_str());

    let plugin = build_real_plugin();
    assert!(plugin.is_file(), "fixture library must exist: {}", plugin.display());

    let id = Factory::load_plugin_file_with_config(&plugin, PluginRuntimeConfig::default()).expect(
        "Factory must load the fixture through ProcessExecutor"
    );
    assert_eq!(Factory::loaded_plugin_names(), vec!["PluginHostIntegration"]);

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
