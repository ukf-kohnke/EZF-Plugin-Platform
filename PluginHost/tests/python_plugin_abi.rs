//! Opt-in cross-language test for the Python ABI V2 sample.
//!
//! Run with:
//!   EZF_RUN_PYTHON_PLUGIN_ABI_TEST=1 cargo test -p PluginHost --test python_plugin_abi -- --nocapture
//!
//! The native shim embeds CPython because the current runtime loads shared libraries through
//! InstantiateV2; the endpoint implementation itself is Python code.

use PluginContracts::ApiContract::{ PluginEndpointRequest, PluginHttpMethod, PluginPrincipal };
use PluginHost::PluginHostInstance::{PluginHostConfig, PluginHostInstance};
use std::{ env, fs, path::{ Path, PathBuf }, process::Command };

fn find_library_recursive(dir: &Path, expected_name: &str) -> Option<PathBuf> {
    let entries = fs::read_dir(dir).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = find_library_recursive(&path, expected_name) {
                return Some(found);
            }
        } else if path.file_name().and_then(|name| name.to_str()) == Some(expected_name) {
            return Some(path);
        }
    }
    None
}

fn platform_library_name() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "libPythonAbiSmokeTest.so"
    }
    #[cfg(target_os = "macos")]
    {
        "libPythonAbiSmokeTest.dylib"
    }
    #[cfg(target_os = "windows")]
    {
        "PythonAbiSmokeTest.dll"
    }
}

fn build_python_plugin() -> (PathBuf, PathBuf) {
    let source_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../Plugins/PythonAbiSmokeTest"
    );
    assert!(source_dir.join("CMakeLists.txt").is_file(), "Python fixture must exist");

    let build_dir = env::temp_dir().join(format!("ezf-python-abi-{}", std::process::id()));
    let _ = fs::remove_dir_all(&build_dir);
    fs::create_dir_all(&build_dir).expect("temporary CMake directory must be creatable");

    let cmake = env::var_os("CMAKE").unwrap_or_else(|| "cmake".into());
    let configure = Command::new(&cmake)
        .arg("-S")
        .arg(&source_dir)
        .arg("-B")
        .arg(&build_dir)
        .arg("-DCMAKE_BUILD_TYPE=Release")
        .status()
        .expect("CMake must be installed when EZF_RUN_PYTHON_PLUGIN_ABI_TEST=1");
    assert!(
        configure.success(),
        "CMake configuration for Python ABI plugin failed; ensure Python development headers/library are installed"
    );

    let build = Command::new(&cmake)
        .arg("--build")
        .arg(&build_dir)
        .arg("--config")
        .arg("Release")
        .status()
        .expect("CMake build command must be executable");
    assert!(build.success(), "Python ABI smoke plugin failed to build");

    let library = find_library_recursive(&build_dir, platform_library_name()).expect(
        "CMake build must produce the expected shared library"
    );
    (library, build_dir)
}

fn ping_request() -> PluginEndpointRequest {
    PluginEndpointRequest {
        endpoint_id: "python-abi-ping".to_owned(),
        method: PluginHttpMethod::Get,
        raw_path: "/python-abi-smoke/ping".to_owned(),
        base_path: "/".to_owned(),
        raw_query: None,
        content_type: None,
        body: Vec::new(),
        principal: PluginPrincipal {
            user_id: "python-abi-test".to_owned(),
            username: "python-abi-test".to_owned(),
            given_name: "Python".to_owned(),
            family_name: "ABI Test".to_owned(),
            groups: Vec::new(),
            session_id: "python-abi-test-session".to_owned(),
        },
    }
}

#[test]
fn python_handler_crosses_the_real_rust_plugin_abi() {
    if env::var_os("EZF_RUN_PYTHON_PLUGIN_ABI_TEST").is_none() {
        eprintln!("skipped; set EZF_RUN_PYTHON_PLUGIN_ABI_TEST=1 to build/load the Python fixture");
        return;
    }

    let (library, build_dir) = build_python_plugin();
    let host = PluginHostInstance::new(PluginHostConfig::default());
    let plugin_id = host.load_plugin_file(&library).expect(
        "Rust PluginHost must load the Python adapter through InstantiateV2"
    );

    let response = host.handle_endpoint(ping_request()).expect(
        "Rust PluginHost must invoke the Python handler over ABI V2"
    );
    assert_eq!(response.status, 200, "Python endpoint must return HTTP 200");

    let body_text = String::from_utf8_lossy(&response.body);
    let body: serde_json::Value = serde_json
        ::from_slice(&response.body)
        .unwrap_or_else(|error| {
            panic!(
                "Python plugin response body must be valid JSON; body={body_text:?}; error={error}"
            )
        });
    assert!(body.is_object(), "expected a JSON object; body={body_text}");
    assert_eq!(body["plugin"], "PythonAbiSmokeTest", "unexpected response body: {body_text}");
    assert_eq!(body["language"], "Python");
    assert_eq!(body["abi_version"], 2);
    assert_eq!(body["endpoint"], "ping");
    assert_eq!(body["loaded"], true);
    assert!(body["python_version"].as_str().is_some(), "Python runtime version must be included");

    host.unload_plugin(plugin_id).expect("Python plugin must unload cleanly through ABI V2");
    let _ = fs::remove_dir_all(build_dir);
}
