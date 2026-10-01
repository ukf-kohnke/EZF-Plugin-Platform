use PluginContracts::ApiContract::{ PluginHttpMethod, PluginEndpointRequest, PluginPrincipal };
use PluginHost::Factory::{ self, PluginLoadPolicy, PluginManagerError, PluginRequirement };
use std::{ fs, path::PathBuf, process::Command, sync::Mutex };

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

    let temp_dir = std::env
        ::temp_dir()
        .join(format!("ezf-pluginhost-dylib-{}", std::process::id()));
    let target_dir = temp_dir.join("target");
    fs::create_dir_all(&target_dir).expect("fixture target directory must be creatable");

    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = Command::new(cargo)
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

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn cleanup_plugin(id: i64) {
    Factory::unload_plugin(id).expect("test plugin must unload cleanly");
}

#[test]
fn loads_executes_unloads_and_reloads_a_real_dynamic_library() {
    let _test_guard = TEST_LOCK.lock().expect("test lock must not be poisoned");
    let library = build_fixture();
    let id = Factory::load_plugin_file(&library).expect("fixture library must load");

    assert_eq!(Factory::plugin_count(), 1);
    assert_eq!(Factory::loaded_plugin_names(), vec!["PluginHostIntegration"]);

    let response = Factory::handle_endpoint("PluginHostIntegration", ping_request()).expect(
        "dynamic endpoint call must cross the ABI boundary"
    );
    assert_eq!(response.status, 200);
    let body: serde_json::Value = serde_json
        ::from_slice(&response.body)
        .expect("plugin response must contain valid JSON");
    assert_eq!(body["plugin"], "PluginHostIntegration");
    assert_eq!(body["loaded"], true);
    assert_eq!(body["endpoint"], "ping");

    cleanup_plugin(id);
    assert_eq!(Factory::plugin_count(), 0);

    let reloaded_id = Factory::load_plugin_file(&library).expect(
        "the same dynamic library must be reloadable after unload"
    );
    assert_eq!(Factory::plugin_count(), 1);

    let reloaded_response = Factory::handle_endpoint(
        "PluginHostIntegration",
        ping_request()
    ).expect("reloaded dynamic endpoint call must cross the ABI boundary");
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
    let _test_guard = TEST_LOCK.lock().expect("test lock must not be poisoned");
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
