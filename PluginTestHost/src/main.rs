mod process_config;

use DatabaseContracts::{
    DatabaseError,
    DatabaseErrorKind,
    DatabaseRequest,
    DatabaseResponse,
    ExecutionResult,
    ResultSet,
};
use PluginAbi::{ AbiBuffer, AbiSlice, PluginCancellationV2 };
use PluginContracts::ApiContract::{
    PluginEndpointError,
    PluginEndpointRequest,
    PluginEndpointResponse,
    PluginHttpMethod,
    PluginManifest,
    PluginPrincipal,
    PluginSessionContext,
};
use PluginHost::PluginHostInstance::{
    PluginExecutorSelection,
    PluginHostConfig,
    PluginHostInstance,
    PluginRuntimeConfig,
};
use serde::Deserialize;
use serde_json::{ json, Value };
use std::{
    env,
    error::Error,
    fs,
    io::{ self, ErrorKind },
    path::{ Path, PathBuf },
    process,
    sync::{ Arc, Mutex, OnceLock },
    sync::atomic::{ AtomicUsize, Ordering },
    time::{ Duration, Instant },
};

type AppResult<T> = Result<T, Box<dyn Error>>;

fn main() {
    match run() {
        Ok(code) => process::exit(code),
        Err(error) => {
            eprintln!("error: {error}");
            process::exit(2);
        }
    }
}

fn run() -> AppResult<i32> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        print_help();
        return Ok(0);
    };
    if matches!(command.as_str(), "--help" | "-h" | "help") {
        print_help();
        return Ok(0);
    }

    let plugin_text = args.next().ok_or_else(|| cli_error("missing plugin library path"))?;
    let requested_plugin = PathBuf::from(plugin_text);
    let plugin_file = fs
        ::canonicalize(&requested_plugin)
        .map_err(|error| {
            cli_error(
                format!("cannot resolve plugin library '{}': {error}", requested_plugin.display())
            )
        })?;
    if !plugin_file.is_file() {
        return Err(
            Box::new(cli_error(format!("plugin path is not a file: {}", plugin_file.display())))
        );
    }

    let options = parse_options(args)?;
    let external_fixtures = options.database_fixtures
        .as_deref()
        .map(read_database_fixtures)
        .transpose()?;
    match command.as_str() {
        "inspect" => {
            if options.database_plugin.is_some() {
                return Err(
                    Box::new(
                        cli_error(
                            "inspect does not use --database-plugin; use the test command with database.mode=live"
                        )
                    )
                );
            }
            if options.request.is_some() || options.scenario.is_some() {
                return Err(Box::new(cli_error("inspect does not accept --request or --scenario")));
            }
            let fixtures = external_fixtures.unwrap_or_default();
            fixtures.validate()?;
            let executor = options.executor.unwrap_or(ExecutorArg::InProcess);
            let (output, cleanup_ok) = run_single_plugin(
                &plugin_file,
                executor,
                runtime_config(&PluginTimeouts::default(), &options),
                fixtures,
                |_, plugin_id, manifest| Ok(manifest_json(plugin_id, manifest, executor))
            )?;
            print_json_output(&output, options.output.as_deref())?;
            Ok(if cleanup_ok { 0 } else { 1 })
        }
        "call" => {
            if options.database_plugin.is_some() {
                return Err(
                    Box::new(
                        cli_error(
                            "--database-plugin is currently supported for 'test' scenarios; use database.mode=live"
                        )
                    )
                );
            }
            if options.scenario.is_some() {
                return Err(Box::new(cli_error("call does not accept --scenario")));
            }
            let request_path = options.request
                .as_ref()
                .ok_or_else(|| cli_error("call requires --request <request.json>"))?;
            let request = read_json_request(request_path)?;
            let fixtures = external_fixtures.unwrap_or_default();
            fixtures.validate()?;
            let executor = options.executor.unwrap_or(ExecutorArg::InProcess);
            let timeouts = PluginTimeouts::default();
            let (output, cleanup_ok) = run_single_plugin(
                &plugin_file,
                executor,
                runtime_config(&timeouts, &options),
                fixtures,
                |host, plugin_id, manifest| {
                    let endpoint_request = request.clone().into_endpoint_request()?;
                    let endpoint_id = endpoint_request.endpoint_id.clone();
                    let method = endpoint_request.method;
                    let path = endpoint_request.raw_path.clone();
                    let outcome = call_endpoint(host, endpoint_request);
                    Ok(
                        json!({
                        "plugin": plugin_identity(plugin_id, manifest, executor),
                        "available_endpoints": endpoints_json(manifest),
                        "request": { "endpoint_id": endpoint_id, "method": method.as_str(), "path": path },
                        "outcome": outcome,
                        "health_after_call": health_json(host, &manifest.name),
                        "database_calls": database_call_snapshot(),
                    })
                    )
                }
            )?;
            print_json_output(&output, options.output.as_deref())?;
            Ok(if cleanup_ok { 0 } else { 1 })
        }
        "test" => {
            if options.request.is_some() {
                return Err(
                    Box::new(
                        cli_error("test does not accept --request; put requests in the scenario")
                    )
                );
            }
            let scenario_path = options.scenario
                .as_ref()
                .ok_or_else(|| cli_error("test requires --scenario <scenario.json>"))?;
            let bytes = fs
                ::read(scenario_path)
                .map_err(|error|
                    cli_error(
                        format!("cannot read scenario '{}': {error}", scenario_path.display())
                    )
                )?;
            let scenario: TestSuite = serde_json
                ::from_slice(&bytes)
                .map_err(|error|
                    cli_error(
                        format!("invalid scenario JSON '{}': {error}", scenario_path.display())
                    )
                )?;
            if scenario.tests.is_empty() {
                return Err(Box::new(cli_error("scenario must contain at least one test case")));
            }
            let live_database = scenario.database.mode
                .as_deref()
                .is_some_and(|mode| mode.eq_ignore_ascii_case("live"));
            if live_database && options.database_fixtures.is_some() {
                return Err(
                    Box::new(
                        cli_error("database.mode=live cannot be combined with --database-fixtures")
                    )
                );
            }
            let fixtures = external_fixtures.unwrap_or(scenario.database.clone());
            fixtures.validate()?;
            let executor = options.executor
                .or(scenario.executor.as_deref().map(parse_executor).transpose()?)
                .unwrap_or(ExecutorArg::InProcess);
            if live_database {
                let db_plugin = options.database_plugin
                    .as_deref()
                    .ok_or_else(||
                        cli_error(
                            "database.mode=live requires --database-plugin <MolgenDB-library>"
                        )
                    )?;
                let runtime = runtime_config(&scenario.timeouts, &options);
                start_live_database_plugin(db_plugin, executor, runtime)?;
                let report_result = run_test_suite(
                    &plugin_file,
                    &scenario,
                    executor,
                    &options,
                    fixtures,
                    true
                );
                let db_cleanup = stop_live_database_plugin();
                let mut report = report_result?;
                add_database_lifecycle_result(&mut report, db_cleanup);
                let passed = report["summary"]["failed"].as_u64().unwrap_or(1) == 0;
                print_json_output(&report, options.output.as_deref())?;
                Ok(if passed { 0 } else { 1 })
            } else {
                if options.database_plugin.is_some() {
                    return Err(
                        Box::new(
                            cli_error("--database-plugin requires scenario database.mode=live")
                        )
                    );
                }
                let test_report = run_test_suite(
                    &plugin_file,
                    &scenario,
                    executor,
                    &options,
                    fixtures,
                    false
                )?;
                let passed = test_report["summary"]["failed"].as_u64().unwrap_or(1) == 0;
                print_json_output(&test_report, options.output.as_deref())?;
                Ok(if passed { 0 } else { 1 })
            }
        }
        other => Err(Box::new(cli_error(format!("unknown command '{other}'")))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutorArg {
    InProcess,
    Process,
}

impl ExecutorArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::InProcess => "in_process",
            Self::Process => "process",
        }
    }

    fn host_selection(self) -> PluginExecutorSelection {
        match self {
            Self::InProcess => PluginExecutorSelection::InProcess,
            Self::Process => PluginExecutorSelection::Process,
        }
    }
}

fn parse_executor(value: &str) -> AppResult<ExecutorArg> {
    match value.trim().to_ascii_lowercase().as_str() {
        "in-process" | "in_process" | "inprocess" => Ok(ExecutorArg::InProcess),
        "process" => Ok(ExecutorArg::Process),
        _ =>
            Err(
                Box::new(
                    cli_error(format!("unknown executor '{value}'; use in-process or process"))
                )
            ),
    }
}

#[derive(Debug, Default)]
struct CliOptions {
    request: Option<PathBuf>,
    scenario: Option<PathBuf>,
    output: Option<PathBuf>,
    database_fixtures: Option<PathBuf>,
    database_plugin: Option<PathBuf>,
    executor: Option<ExecutorArg>,
    timeout_ms: Option<u64>,
    unload_timeout_ms: Option<u64>,
    lifecycle_timeout_ms: Option<u64>,
}

fn parse_options(mut args: impl Iterator<Item = String>) -> AppResult<CliOptions> {
    let mut options = CliOptions::default();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--request" | "-r" => set_path(&mut options.request, "--request", args.next())?,
            "--scenario" | "-s" => set_path(&mut options.scenario, "--scenario", args.next())?,
            "--output" | "-o" => set_path(&mut options.output, "--output", args.next())?,
            "--database-fixtures" | "--db-fixtures" =>
                set_path(&mut options.database_fixtures, "--database-fixtures", args.next())?,
            "--database-plugin" =>
                set_path(&mut options.database_plugin, "--database-plugin", args.next())?,
            "--executor" => {
                let value = args
                    .next()
                    .ok_or_else(|| cli_error("--executor requires in-process or process"))?;
                options.executor = Some(parse_executor(&value)?);
            }
            "--timeout-ms" => {
                options.timeout_ms = Some(parse_positive_u64("--timeout-ms", args.next())?);
            }
            "--unload-timeout-ms" => {
                options.unload_timeout_ms = Some(
                    parse_positive_u64("--unload-timeout-ms", args.next())?
                );
            }
            "--lifecycle-timeout-ms" => {
                options.lifecycle_timeout_ms = Some(
                    parse_positive_u64("--lifecycle-timeout-ms", args.next())?
                );
            }
            "--format" => {
                let format = args.next().ok_or_else(|| cli_error("--format requires json"))?;
                if !format.eq_ignore_ascii_case("json") {
                    return Err(Box::new(cli_error("only --format json is supported in Phase 2")));
                }
            }
            "--help" | "-h" => {
                print_help();
                return Ok(options);
            }
            other => {
                return Err(Box::new(cli_error(format!("unknown argument '{other}'"))));
            }
        }
    }
    Ok(options)
}

fn set_path(slot: &mut Option<PathBuf>, flag: &str, value: Option<String>) -> AppResult<()> {
    if slot.is_some() {
        return Err(Box::new(cli_error(format!("{flag} may be specified only once"))));
    }
    *slot = Some(PathBuf::from(value.ok_or_else(|| cli_error(format!("{flag} requires a path")))?));
    Ok(())
}

fn parse_positive_u64(flag: &str, value: Option<String>) -> AppResult<u64> {
    let value = value.ok_or_else(||
        cli_error(format!("{flag} requires a number of milliseconds"))
    )?;
    let parsed = value
        .parse::<u64>()
        .map_err(|_| cli_error(format!("{flag} requires a positive integer")))?;
    if parsed == 0 || parsed > 600_000 {
        return Err(
            Box::new(cli_error(format!("{flag} must be between 1 and 600000 milliseconds")))
        );
    }
    Ok(parsed)
}

#[derive(Debug, Clone, Deserialize)]
struct JsonRequest {
    endpoint_id: String,
    method: String,
    #[serde(alias = "raw_path")]
    path: String,
    #[serde(default = "default_base_path")]
    base_path: String,
    #[serde(default, alias = "query")]
    raw_query: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    body: Option<Value>,
    #[serde(default)]
    principal: Option<PluginPrincipal>,
}

impl JsonRequest {
    fn into_endpoint_request(self) -> AppResult<PluginEndpointRequest> {
        let method = parse_method(&self.method)?;
        let body = match self.body {
            Some(value) => serde_json::to_vec(&value)?,
            None => Vec::new(),
        };
        let content_type = self.content_type.or_else(|| {
            if body.is_empty() { None } else { Some("application/json".to_owned()) }
        });
        Ok(PluginEndpointRequest {
            endpoint_id: self.endpoint_id,
            method,
            raw_path: self.path,
            base_path: self.base_path,
            raw_query: self.raw_query,
            content_type,
            body,
            principal: self.principal.unwrap_or_else(default_principal),
        })
    }
}

fn read_json_request(path: &Path) -> AppResult<JsonRequest> {
    let bytes = fs
        ::read(path)
        .map_err(|error|
            cli_error(format!("cannot read request file '{}': {error}", path.display()))
        )?;
    serde_json
        ::from_slice(&bytes)
        .map_err(
            |error|
                Box::new(
                    cli_error(format!("invalid request JSON '{}': {error}", path.display()))
                ) as Box<dyn Error>
        )
}

fn default_base_path() -> String {
    "/".to_owned()
}

fn default_principal() -> PluginPrincipal {
    PluginPrincipal {
        user_id: "test-user".to_owned(),
        username: "test-user".to_owned(),
        given_name: "Plugin".to_owned(),
        family_name: "Test".to_owned(),
        groups: vec!["plugin-test".to_owned()],
        session_id: "plugin-test-session".to_owned(),
    }
}

fn parse_method(value: &str) -> AppResult<PluginHttpMethod> {
    match value.trim().to_ascii_uppercase().as_str() {
        "GET" => Ok(PluginHttpMethod::Get),
        "POST" => Ok(PluginHttpMethod::Post),
        "PUT" => Ok(PluginHttpMethod::Put),
        "PATCH" => Ok(PluginHttpMethod::Patch),
        "DELETE" => Ok(PluginHttpMethod::Delete),
        _ =>
            Err(
                Box::new(
                    cli_error(
                        format!(
                            "unsupported HTTP method '{value}'; use GET, POST, PUT, PATCH, or DELETE"
                        )
                    )
                )
            ),
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct PluginTimeouts {
    call_ms: Option<u64>,
    unload_ms: Option<u64>,
    lifecycle_ms: Option<u64>,
}

impl Default for PluginTimeouts {
    fn default() -> Self {
        Self { call_ms: None, unload_ms: None, lifecycle_ms: None }
    }
}

impl PluginTimeouts {
    fn validate(&self) -> AppResult<()> {
        for (name, value) in [
            ("call_ms", self.call_ms),
            ("unload_ms", self.unload_ms),
            ("lifecycle_ms", self.lifecycle_ms),
        ] {
            if let Some(value) = value {
                if value == 0 || value > 600_000 {
                    return Err(
                        Box::new(
                            cli_error(
                                format!(
                                    "scenario timeout '{name}' must be between 1 and 600000 milliseconds"
                                )
                            )
                        )
                    );
                }
            }
        }
        Ok(())
    }
}

fn runtime_config(timeouts: &PluginTimeouts, options: &CliOptions) -> PluginRuntimeConfig {
    let mut runtime = PluginRuntimeConfig::default();
    runtime.call_timeout = Duration::from_millis(
        options.timeout_ms.or(timeouts.call_ms).unwrap_or(runtime.call_timeout.as_millis() as u64)
    );
    runtime.unload_timeout = Duration::from_millis(
        options.unload_timeout_ms
            .or(timeouts.unload_ms)
            .unwrap_or(runtime.unload_timeout.as_millis() as u64)
    );
    runtime.lifecycle_timeout = Duration::from_millis(
        options.lifecycle_timeout_ms
            .or(timeouts.lifecycle_ms)
            .unwrap_or(runtime.lifecycle_timeout.as_millis() as u64)
    );
    runtime
}

fn run_single_plugin<F>(
    plugin_file: &Path,
    executor: ExecutorArg,
    runtime: PluginRuntimeConfig,
    fixtures: DatabaseFixtures,
    operation: F
) -> AppResult<(Value, bool)>
    where F: FnOnce(&PluginHostInstance, i64, &PluginManifest) -> AppResult<Value>
{
    configure_database_mock(fixtures);
    let parent = plugin_file.parent().unwrap_or_else(|| Path::new("."));
    let process_executor = process_config
        ::from_environment(executor == ExecutorArg::Process)
        .map_err(cli_error)?;
    let host = PluginHostInstance::new(PluginHostConfig {
        plugin_root: parent.to_path_buf(),
        runtime,
        executor: executor.host_selection(),
        process_executor,
    });
    host
        .register_database_executor(mock_database_execute, mock_database_free)
        .map_err(|error| cli_error(format!("cannot install database mock: {error}")))?;
    let plugin_id = host
        .load_plugin_file(plugin_file)
        .map_err(|error| {
            cli_error(
                format!("plugin failed ABI/manifest validation or could not be loaded: {error}")
            )
        })?;
    let manifest = match host.plugin_manifests().as_slice() {
        [manifest] => manifest.clone(),
        manifests => {
            let count = manifests.len();
            let unload = host.unload_plugin(plugin_id);
            let detail = format!("expected exactly one loaded plugin manifest, found {count}");
            unload.map_err(|error| cli_error(format!("{detail}; cleanup failed: {error}")))?;
            return Err(Box::new(cli_error(detail)));
        }
    };
    let operation_result = operation(&host, plugin_id, &manifest);
    let health_before_unload = health_json(&host, &manifest.name);
    let db_calls = database_call_snapshot();
    let unload_result = host.unload_plugin(plugin_id);
    let cleanup = match unload_result {
        Ok(()) => json!({"success": true}),
        Err(error) => json!({"success": false, "error": error.to_string()}),
    };
    let mut result = operation_result?;
    if let Some(object) = result.as_object_mut() {
        object.insert("cleanup".to_owned(), cleanup.clone());
        object.insert("health_before_unload".to_owned(), health_before_unload);
        object.insert("database_calls".to_owned(), Value::Array(db_calls));
    }
    Ok((result, cleanup["success"].as_bool().unwrap_or(false)))
}

fn manifest_json(plugin_id: i64, manifest: &PluginManifest, executor: ExecutorArg) -> Value {
    json!({
        "loaded": true,
        "plugin": plugin_identity(plugin_id, manifest, executor),
        "endpoints": endpoints_json(manifest),
    })
}

fn plugin_identity(plugin_id: i64, manifest: &PluginManifest, executor: ExecutorArg) -> Value {
    let mut identity = json!({
        "id": plugin_id,
        "name": manifest.name,
        "version": manifest.version,
        "contract_version": manifest.contract_version,
        "kind": manifest.kind,
        "abi": "V2",
        "executor": executor.as_str(),
    });
    if let Some(description) = manifest.description.as_deref() {
        identity["description"] = Value::String(description.to_owned());
    }
    identity
}

fn endpoints_json(manifest: &PluginManifest) -> Vec<Value> {
    manifest.endpoints
        .iter()
        .map(
            |endpoint|
                json!({
        "id": endpoint.id,
        "method": endpoint.method.as_str(),
        "path": endpoint.path,
        "summary": endpoint.summary,
        "description": endpoint.description,
        "consumes": endpoint.consumes,
        "produces": endpoint.produces,
        "query_parameters": endpoint.query_parameters,
    })
        )
        .collect()
}

fn call_endpoint(host: &PluginHostInstance, request: PluginEndpointRequest) -> Value {
    match host.handle_loaded_endpoint(request) {
        Ok(response) =>
            json!({
            "kind": "response",
            "status": response.status,
            "content_type": response.content_type,
            "headers": response.headers,
            "body": body_json(&response),
        }),
        Err(error) => endpoint_error_json(&error),
    }
}

fn body_json(response: &PluginEndpointResponse) -> Value {
    if response.body.is_empty() {
        return Value::Null;
    }
    match serde_json::from_slice::<Value>(&response.body) {
        Ok(value) => value,
        Err(_) =>
            match String::from_utf8(response.body.clone()) {
                Ok(text) => Value::String(text),
                Err(_) => json!({"non_utf8_body_bytes": response.body.len()}),
            }
    }
}

fn endpoint_error_json(error: &PluginEndpointError) -> Value {
    json!({
        "kind": "error",
        "status": error.status,
        "code": error.code,
        "message": error.user_message,
        "detailed_message": error.detailed_message,
    })
}

/// Text searched by `expect.error_contains`. This supports both host-level errors (such as a
/// timeout) and valid HTTP error responses emitted by the plugin (for example a JSON 400 body).
fn endpoint_error_text_for_assertion(outcome: &Value) -> Option<String> {
    if outcome.get("kind").and_then(Value::as_str) == Some("error") {
        return Some(serde_json::to_string(outcome).unwrap_or_else(|_| format!("{outcome:?}")));
    }
    let status = outcome.get("status").and_then(Value::as_u64).unwrap_or(200);
    if status >= 400 {
        return outcome.get("body").map(|body| body.to_string());
    }
    None
}

fn health_json(host: &PluginHostInstance, name: &str) -> Value {
    match host.plugin_health_by_name(name) {
        Some(health) =>
            json!({
            "name": health.name,
            "state": format!("{:?}", health.state),
            "executor": health.executor.as_str(),
            "worker_pid": health.worker_pid,
            "worker_threads": health.worker_threads,
            "max_concurrent_calls": health.max_concurrent_calls,
            "max_parallel_plugin_calls": health.max_parallel_plugin_calls,
            "queue_capacity": health.queue_capacity,
            "queued_jobs": health.queued_jobs,
            "running_workers": health.running_workers,
            "active_calls": health.active_calls,
            "timed_out_active_calls": health.timed_out_active_calls,
            "completed_calls": health.completed_calls,
            "failed_calls": health.failed_calls,
            "timed_out_calls": health.timed_out_calls,
            "total_calls": health.total_calls,
            "rejected_calls": health.rejected_calls,
            "last_error": health.last_error,
        }),
        None => Value::Null,
    }
}

fn print_json_output(value: &Value, output: Option<&Path>) -> AppResult<()> {
    let serialized = serde_json::to_string_pretty(value)?;
    if let Some(path) = output {
        fs
            ::write(path, format!("{serialized}\n"))
            .map_err(|error|
                cli_error(format!("cannot write JSON report '{}': {error}", path.display()))
            )?;
    }
    println!("{serialized}");
    Ok(())
}

// ------------------------------ Phase 2 test suites ------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct TestSuite {
    name: Option<String>,
    executor: Option<String>,
    timeouts: PluginTimeouts,
    principal: Option<PluginPrincipal>,
    database: DatabaseFixtures,
    tests: Vec<TestCase>,
    expect_unload_success: Option<bool>,
}

impl Default for TestSuite {
    fn default() -> Self {
        Self {
            name: None,
            executor: None,
            timeouts: PluginTimeouts::default(),
            principal: None,
            database: DatabaseFixtures::default(),
            tests: Vec::new(),
            expect_unload_success: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct TestCase {
    name: String,
    #[serde(default)]
    request: Option<JsonRequest>,
    #[serde(default, alias = "notify_session_closed")]
    session_closed: bool,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    expect: TestExpectation,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct TestExpectation {
    status: Option<u16>,
    body_equals: Option<Value>,
    body_json_path_equals: Option<JsonPathEquality>,
    error_contains: Option<String>,
    session_close_success: Option<bool>,
    health_state: Option<String>,
    database_call_count: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct JsonPathEquality {
    path: String,
    equals: Value,
}

fn run_test_suite(
    plugin_file: &Path,
    suite: &TestSuite,
    executor: ExecutorArg,
    options: &CliOptions,
    fixtures: DatabaseFixtures,
    live_database: bool
) -> AppResult<Value> {
    fixtures.validate()?;
    suite.timeouts.validate()?;
    // Validate all request envelopes before loading the plugin. Invalid scenario input must not
    // leave a live plugin instance behind because request conversion failed mid-suite.
    for (index, case) in suite.tests.iter().enumerate() {
        if case.name.trim().is_empty() {
            return Err(
                Box::new(cli_error(format!("scenario test {} has an empty name", index + 1)))
            );
        }
        if let Some(request) = case.request.clone() {
            let _ = request.into_endpoint_request()?;
        }
    }
    if !live_database {
        configure_database_mock(fixtures);
    }
    let runtime = runtime_config(&suite.timeouts, options);
    let process_executor = process_config
        ::from_environment(executor == ExecutorArg::Process)
        .map_err(cli_error)?;
    let host = PluginHostInstance::new(PluginHostConfig {
        plugin_root: plugin_file
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf(),
        runtime,
        executor: executor.host_selection(),
        process_executor,
    });
    let registration = if live_database {
        host.register_database_executor(live_database_execute, mock_database_free)
    } else {
        host.register_database_executor(mock_database_execute, mock_database_free)
    };
    registration.map_err(|error|
        cli_error(
            format!("cannot install {} database capability: {error}", if live_database {
                "live"
            } else {
                "mock"
            })
        )
    )?;

    let suite_name = suite.name.clone().unwrap_or_else(||
        plugin_file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("plugin-test-suite")
            .to_owned()
    );
    let started = Instant::now();
    let plugin_id = match host.load_plugin_file(plugin_file) {
        Ok(id) => id,
        Err(error) => {
            let setup_error = format!(
                "plugin failed ABI/manifest validation or could not be loaded: {error}"
            );
            let results = suite.tests
                .iter()
                .map(
                    |test|
                        json!({
                "name": test.name,
                "passed": false,
                "duration_ms": 0,
                "error": setup_error.clone(),
                "checks": [{"name":"plugin_load", "passed": false, "detail": setup_error.clone()}],
            })
                )
                .collect::<Vec<_>>();
            return Ok(
                build_suite_report(
                    &suite_name,
                    plugin_file,
                    executor,
                    runtime,
                    results,
                    json!({"success":false,"error":setup_error}),
                    started.elapsed().as_millis() as u64
                )
            );
        }
    };
    let manifest = match host.plugin_manifests().as_slice() {
        [manifest] => manifest.clone(),
        manifests => {
            let count = manifests.len();
            let unload = host.unload_plugin(plugin_id);
            let detail = format!("expected exactly one loaded manifest, found {count}");
            unload.map_err(|error| cli_error(format!("{detail}; cleanup failed: {error}")))?;
            return Err(Box::new(cli_error(detail)));
        }
    };

    let mut results = Vec::<Value>::new();
    let mut db_cursor = 0usize;
    for case in &suite.tests {
        let case_started = Instant::now();
        let mut outcome = Value::Null;
        let mut effective_principal: Option<Value> = None;
        let mut call_status: Option<u16> = None;
        let mut response_body: Option<Value> = None;
        let mut endpoint_error_text: Option<String> = None;
        let mut performed_action = false;
        if let Some(input) = case.request.clone() {
            performed_action = true;
            let specified_principal = input.principal.is_some();
            let mut request = input.into_endpoint_request()?;
            if !specified_principal {
                if let Some(principal) = suite.principal.clone() {
                    request.principal = principal;
                }
            }
            effective_principal = Some(
                json!({
                "user_id": request.principal.user_id.clone(),
                "username": request.principal.username.clone(),
                "given_name": request.principal.given_name.clone(),
                "family_name": request.principal.family_name.clone(),
                "groups": request.principal.groups.clone(),
                "session_id": request.principal.session_id.clone(),
            })
            );
            let result = call_endpoint(&host, request);
            call_status = result
                .get("status")
                .and_then(Value::as_u64)
                .map(|v| v as u16);
            response_body = result.get("body").cloned();
            endpoint_error_text = endpoint_error_text_for_assertion(&result);
            outcome = result;
        }

        let session_close_result = if case.session_closed {
            performed_action = true;
            let session_id = case.session_id
                .clone()
                .or_else(||
                    case.request
                        .as_ref()
                        .and_then(|request|
                            request.principal.as_ref().map(|principal| principal.session_id.clone())
                        )
                )
                .or_else(|| suite.principal.as_ref().map(|principal| principal.session_id.clone()))
                .unwrap_or_else(|| default_principal().session_id);
            let report = host.notify_session_closed(&PluginSessionContext::new(session_id.clone()));
            Some(
                json!({
                "success": report.is_success(),
                "session_id": session_id,
                "notified_plugins": report.notified_plugins,
                "failures": report.failures.iter().map(|failure| json!({
                    "plugin_name": failure.plugin_name,
                    "code": failure.error.code,
                    "message": failure.error.message,
                })).collect::<Vec<_>>(),
            })
            )
        } else {
            None
        };

        let health = health_json(&host, &manifest.name);
        let db_calls_all = database_call_snapshot();
        let safe_cursor = db_cursor.min(db_calls_all.len());
        let db_calls_this_case = db_calls_all[safe_cursor..].to_vec();
        db_cursor = db_calls_all.len();
        let mut checks = Vec::<Value>::new();
        let has_explicit_expectation =
            case.expect.status.is_some() ||
            case.expect.body_equals.is_some() ||
            case.expect.body_json_path_equals.is_some() ||
            case.expect.error_contains.is_some() ||
            case.expect.session_close_success.is_some() ||
            case.expect.health_state.is_some() ||
            case.expect.database_call_count.is_some();

        if let Some(expected) = case.expect.status {
            let passed = call_status == Some(expected);
            checks.push(
                check_json("status", passed, format!("expected {expected}, got {call_status:?}"))
            );
        }
        if let Some(expected) = &case.expect.body_equals {
            let passed =
                outcome["kind"].as_str() == Some("response") &&
                response_body.as_ref() == Some(expected);
            checks.push(
                check_json(
                    "body_equals",
                    passed,
                    format!(
                        "expected {expected}, got {}",
                        response_body
                            .as_ref()
                            .map(Value::to_string)
                            .unwrap_or_else(|| "<no response body>".to_owned())
                    )
                )
            );
        }
        if let Some(expected) = &case.expect.body_json_path_equals {
            let actual = response_body
                .as_ref()
                .and_then(|body| json_path_value(body, &expected.path));
            let passed =
                outcome["kind"].as_str() == Some("response") && actual == Some(&expected.equals);
            checks.push(
                check_json(
                    "body_json_path_equals",
                    passed,
                    format!(
                        "expected body path {:?} = {}, got {}",
                        expected.path,
                        expected.equals,
                        actual.map(Value::to_string).unwrap_or_else(|| "<missing>".to_owned())
                    )
                )
            );
        }
        if let Some(expected) = &case.expect.error_contains {
            let actual = endpoint_error_text.as_deref().unwrap_or("");
            let passed = actual.to_ascii_lowercase().contains(&expected.to_ascii_lowercase());
            checks.push(
                check_json(
                    "error_contains",
                    passed,
                    format!("expected error to contain {expected:?}, got {actual:?}")
                )
            );
        }
        if case.session_closed {
            let actual = session_close_result
                .as_ref()
                .and_then(|v| v["success"].as_bool())
                .unwrap_or(false);
            let expected = case.expect.session_close_success.unwrap_or(true);
            checks.push(
                check_json(
                    "session_close_success",
                    actual == expected,
                    format!("expected {expected}, got {actual}")
                )
            );
        } else if let Some(expected) = case.expect.session_close_success {
            checks.push(
                check_json(
                    "session_close_success",
                    false,
                    format!("expected {expected}, but session_closed was not requested")
                )
            );
        }
        if let Some(expected) = &case.expect.health_state {
            let actual = health["state"].as_str().unwrap_or("unavailable");
            checks.push(
                check_json(
                    "health_state",
                    actual.eq_ignore_ascii_case(expected),
                    format!("expected {expected}, got {actual}")
                )
            );
        }
        if let Some(expected) = case.expect.database_call_count {
            checks.push(
                check_json(
                    "database_call_count",
                    db_calls_this_case.len() == expected,
                    format!("expected {expected}, got {}", db_calls_this_case.len())
                )
            );
        }
        if !has_explicit_expectation {
            let http_ok =
                endpoint_error_text.is_none() &&
                call_status.map(|status| status < 400).unwrap_or(true);
            let action_ok =
                performed_action &&
                http_ok &&
                session_close_result
                    .as_ref()
                    .map(|v| v["success"].as_bool().unwrap_or(false))
                    .unwrap_or(true);
            checks.push(
                check_json("default_success", action_ok, if action_ok {
                    "action completed without a plugin error".to_owned()
                } else {
                    "case had no action or completed with an error".to_owned()
                })
            );
        }
        let passed = checks.iter().all(|check| check["passed"].as_bool().unwrap_or(false));
        results.push(
            json!({
            "name": case.name,
            "passed": passed,
            "duration_ms": case_started.elapsed().as_millis() as u64,
            "effective_principal": effective_principal,
            "request_outcome": outcome,
            "session_close": session_close_result,
            "health_after_case": health,
            "database_calls": db_calls_this_case,
            "checks": checks,
        })
        );
    }

    let health_before_unload = health_json(&host, &manifest.name);
    let unload_result = host.unload_plugin(plugin_id);
    let (unload_success, unload_value) = match unload_result {
        Ok(()) => (true, json!({"success":true})),
        Err(error) => (false, json!({"success":false,"error":error.to_string()})),
    };
    let expected_unload = suite.expect_unload_success.unwrap_or(true);
    let unload_passed = unload_success == expected_unload;
    results.push(
        json!({
        "name": "lifecycle.unload",
        "phase": "lifecycle",
        "passed": unload_passed,
        "duration_ms": 0,
        "expected_success": expected_unload,
        "outcome": unload_value,
        "health_before_unload": health_before_unload,
        "checks": [check_json("unload_success", unload_passed, format!("expected {expected_unload}, got {unload_success}"))],
    })
    );

    Ok(
        build_suite_report(
            &suite_name,
            plugin_file,
            executor,
            runtime,
            results,
            json!({"success":true,"plugin":manifest.name,"plugin_id":plugin_id}),
            started.elapsed().as_millis() as u64
        )
    )
}

fn json_path_value<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        if segment.is_empty() {
            return None;
        }
        current = match current {
            Value::Object(object) => object.get(segment)?,
            Value::Array(items) => items.get(segment.parse::<usize>().ok()?)?,
            _ => {
                return None;
            }
        };
    }
    Some(current)
}

fn check_json(name: &str, passed: bool, detail: String) -> Value {
    json!({"name":name,"passed":passed,"detail":detail})
}

fn build_suite_report(
    suite_name: &str,
    plugin_file: &Path,
    executor: ExecutorArg,
    runtime: PluginRuntimeConfig,
    tests: Vec<Value>,
    setup: Value,
    duration_ms: u64
) -> Value {
    let total = tests.len();
    let passed = tests
        .iter()
        .filter(|test| test["passed"].as_bool() == Some(true))
        .count();
    let failed = total - passed;
    json!({
        "schema_version": 1,
        "suite": suite_name,
        "plugin_path": plugin_file,
        "executor": executor.as_str(),
        "runtime": {
            "call_timeout_ms": runtime.call_timeout.as_millis(),
            "unload_timeout_ms": runtime.unload_timeout.as_millis(),
            "lifecycle_timeout_ms": runtime.lifecycle_timeout.as_millis(),
            "worker_threads": runtime.worker_threads,
            "max_concurrent_calls": runtime.max_concurrent_calls,
            "queue_capacity": runtime.queue_capacity,
        },
        "setup": setup,
        "summary": {"total":total,"passed":passed,"failed":failed,"duration_ms":duration_ms},
        "tests": tests,
        "database_calls": database_call_snapshot(),
    })
}

// ------------------------------ Controlled database mock ------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct DatabaseFixtures {
    /// "mock" (default) uses controlled fixtures; "live" forwards requests to a real DataSource plugin.
    mode: Option<String>,
    default_query: ResultSet,
    default_execute_rows_affected: u64,
    default_error: Option<DatabaseFixtureError>,
    rules: Vec<DatabaseFixtureRule>,
}

impl Default for DatabaseFixtures {
    fn default() -> Self {
        Self {
            mode: None,
            default_query: ResultSet::default(),
            default_execute_rows_affected: 1,
            default_error: None,
            rules: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct DatabaseFixtureRule {
    operation: Option<String>,
    statement_contains: Option<String>,
    call_number: Option<usize>,
    response: Option<DatabaseResponse>,
    error: Option<DatabaseFixtureError>,
}

impl Default for DatabaseFixtureRule {
    fn default() -> Self {
        Self {
            operation: None,
            statement_contains: None,
            call_number: None,
            response: None,
            error: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct DatabaseFixtureError {
    kind: DatabaseErrorKind,
    message: String,
}

impl DatabaseFixtureError {
    fn into_error(self) -> DatabaseError {
        DatabaseError::new(self.kind, self.message)
    }
}

impl DatabaseFixtures {
    fn validate(&self) -> AppResult<()> {
        if let Some(mode) = self.mode.as_deref() {
            if !matches!(mode.trim().to_ascii_lowercase().as_str(), "mock" | "live") {
                return Err(
                    Box::new(cli_error(format!("unknown database mode {mode:?}; use mock or live")))
                );
            }
            if
                mode.eq_ignore_ascii_case("live") &&
                (!self.rules.is_empty() ||
                    self.default_error.is_some() ||
                    !self.default_query.rows.is_empty() ||
                    self.default_execute_rows_affected != 1)
            {
                return Err(
                    Box::new(
                        cli_error(
                            "database.mode=live cannot be combined with mock fixture rules or non-default mock responses"
                        )
                    )
                );
            }
        }
        for (index, rule) in self.rules.iter().enumerate() {
            if rule.response.is_some() == rule.error.is_some() {
                return Err(
                    Box::new(
                        cli_error(
                            format!(
                                "database rule {} must contain exactly one of 'response' or 'error'",
                                index + 1
                            )
                        )
                    )
                );
            }
            if let Some(operation) = &rule.operation {
                if !matches!(operation.to_ascii_lowercase().as_str(), "query" | "execute") {
                    return Err(
                        Box::new(
                            cli_error(
                                format!(
                                    "database rule {} has invalid operation '{}'; use query or execute",
                                    index + 1,
                                    operation
                                )
                            )
                        )
                    );
                }
            }
            if
                rule.operation.is_none() &&
                rule.statement_contains.is_none() &&
                rule.call_number.is_none()
            {
                return Err(
                    Box::new(
                        cli_error(
                            format!(
                                "database rule {} must have at least one match condition",
                                index + 1
                            )
                        )
                    )
                );
            }
            if rule.call_number == Some(0) {
                return Err(
                    Box::new(
                        cli_error(
                            format!(
                                "database rule {} call_number is 1-based and must be greater than zero",
                                index + 1
                            )
                        )
                    )
                );
            }
            if rule.statement_contains.as_deref() == Some("") {
                return Err(
                    Box::new(
                        cli_error(
                            format!(
                                "database rule {} statement_contains must not be empty",
                                index + 1
                            )
                        )
                    )
                );
            }
            if
                let (Some(operation), Some(response)) = (
                    rule.operation.as_deref(),
                    rule.response.as_ref(),
                )
            {
                let response_matches = matches!(
                    (operation.to_ascii_lowercase().as_str(), response),
                    ("query", DatabaseResponse::Query(_)) |
                        ("execute", DatabaseResponse::Execute(_))
                );
                if !response_matches {
                    return Err(
                        Box::new(
                            cli_error(
                                format!(
                                    "database rule {} response type does not match operation '{}'",
                                    index + 1,
                                    operation
                                )
                            )
                        )
                    );
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct DatabaseMockState {
    fixtures: DatabaseFixtures,
    calls: Vec<Value>,
}

static DATABASE_MOCK_STATE: OnceLock<Mutex<DatabaseMockState>> = OnceLock::new();

fn database_mock_state() -> &'static Mutex<DatabaseMockState> {
    DATABASE_MOCK_STATE.get_or_init(|| Mutex::new(DatabaseMockState::default()))
}

fn configure_database_mock(fixtures: DatabaseFixtures) {
    let mut state = database_mock_state()
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    state.fixtures = fixtures;
    state.calls.clear();
}

fn database_call_snapshot() -> Vec<Value> {
    if
        let Some(state) = live_database_state()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
    {
        let mut calls = state.calls
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        calls.sort_by_key(|item| item["call_number"].as_u64().unwrap_or(0));
        return calls;
    }
    database_mock_state()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .calls.clone()
}

fn resolve_database_fixture(
    fixtures: &DatabaseFixtures,
    request: &DatabaseRequest,
    call_number: usize
) -> Result<DatabaseResponse, DatabaseError> {
    let (operation, statement) = match request {
        DatabaseRequest::Query(query) => ("query", query.statement.as_str()),
        DatabaseRequest::Execute(execute) => ("execute", execute.statement.as_str()),
    };
    for rule in &fixtures.rules {
        let operation_matches = rule.operation
            .as_deref()
            .map(|value| value.eq_ignore_ascii_case(operation))
            .unwrap_or(true);
        let statement_matches = rule.statement_contains
            .as_deref()
            .map(|value| statement.contains(value))
            .unwrap_or(true);
        let number_matches = rule.call_number.map(|value| value == call_number).unwrap_or(true);
        if operation_matches && statement_matches && number_matches {
            if let Some(error) = rule.error.clone() {
                return Err(error.into_error());
            }
            if let Some(response) = rule.response.clone() {
                let response_matches = matches!(
                    (operation, &response),
                    ("query", DatabaseResponse::Query(_)) |
                        ("execute", DatabaseResponse::Execute(_))
                );
                if response_matches {
                    return Ok(response);
                }
                return Err(
                    DatabaseError::new(
                        DatabaseErrorKind::InvalidRequest,
                        format!(
                            "database fixture response type does not match request operation '{operation}'"
                        )
                    )
                );
            }
        }
    }
    if let Some(error) = fixtures.default_error.clone() {
        return Err(error.into_error());
    }
    Ok(match request {
        DatabaseRequest::Query(_) => DatabaseResponse::Query(fixtures.default_query.clone()),
        DatabaseRequest::Execute(_) =>
            DatabaseResponse::Execute(ExecutionResult {
                rows_affected: fixtures.default_execute_rows_affected,
            }),
    })
}

fn request_operation_and_statement(request: &DatabaseRequest) -> (&'static str, &str) {
    match request {
        DatabaseRequest::Query(query) => ("query", &query.statement),
        DatabaseRequest::Execute(execute) => ("execute", &execute.statement),
    }
}

/// The ABI V2 database callback has no user-data pointer. The CLI therefore owns one explicit,
/// process-local mock state for the lifetime of one test-host invocation; one suite loads one
/// plugin and runs its cases sequentially. This state is not shared with PluginHost or BackendSrv.
unsafe extern "C" fn mock_database_execute(
    request: AbiSlice,
    cancellation: PluginCancellationV2,
    response_out: *mut AbiBuffer
) -> i32 {
    if response_out.is_null() {
        return PluginAbi::PLUGIN_STATUS_SERIALIZATION;
    }
    unsafe {
        *response_out = AbiBuffer::EMPTY;
    }

    let canccelled = unsafe { cancellation.is_cancelled() };
    if canccelled {
        return PluginAbi::PLUGIN_STATUS_CANCELLED;
    }

    let request_vec = unsafe { request.to_vec() };
    let Some(request_bytes) = request_vec else {
        return PluginAbi::PLUGIN_STATUS_SERIALIZATION;
    };

    let database_request = match serde_json::from_slice::<DatabaseRequest>(&request_bytes) {
        Ok(request) => request,
        Err(error) => {
            let result: Result<DatabaseResponse, DatabaseError> = Err(
                DatabaseError::new(
                    DatabaseErrorKind::InvalidRequest,
                    format!("test-host database mock could not decode request: {error}")
                )
            );
            return write_database_response(response_out, &result);
        }
    };
    let request_json = serde_json::to_value(&database_request).unwrap_or(Value::Null);
    let (operation, statement) = request_operation_and_statement(&database_request);
    let mut state = database_mock_state()
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let call_number = state.calls.len() + 1;
    let result = resolve_database_fixture(&state.fixtures, &database_request, call_number);
    let outcome_json = match &result {
        Ok(response) => json!({"ok":true,"response":response}),
        Err(error) => json!({"ok":false,"error":{"kind":error.kind,"message":error.message}}),
    };
    state.calls.push(
        json!({
        "call_number": call_number,
        "operation": operation,
        "statement": statement,
        "request": request_json,
        "outcome": outcome_json,
    })
    );
    drop(state);
    write_database_response(response_out, &result)
}

fn write_database_response(
    response_out: *mut AbiBuffer,
    response: &Result<DatabaseResponse, DatabaseError>
) -> i32 {
    match serde_json::to_vec(response) {
        Ok(bytes) => {
            unsafe {
                *response_out = AbiBuffer::from_vec(bytes);
            }
            PluginAbi::PLUGIN_STATUS_OK
        }
        Err(_) => PluginAbi::PLUGIN_STATUS_SERIALIZATION,
    }
}

unsafe extern "C" fn mock_database_free(buffer: AbiBuffer) {
    unsafe {
        buffer.free();
    }
}

// ------------------------------ Optional live DataSource plugin ------------------------------
// ABI V2 has no user-data pointer on the DB callback. This CLI process therefore exposes one
// explicitly configured database host at a time. The test plugin and the DataSource can each use
// the selected existing executor (InProcess or Process); requests are forwarded through ABI V2.
struct LiveDatabaseState {
    host: Arc<PluginHostInstance>,
    plugin_id: i64,
    calls: Arc<Mutex<Vec<Value>>>,
    next_call: Arc<AtomicUsize>,
}

static LIVE_DATABASE_STATE: OnceLock<Mutex<Option<LiveDatabaseState>>> = OnceLock::new();

fn live_database_state() -> &'static Mutex<Option<LiveDatabaseState>> {
    LIVE_DATABASE_STATE.get_or_init(|| Mutex::new(None))
}

fn start_live_database_plugin(
    path: &Path,
    executor: ExecutorArg,
    runtime: PluginRuntimeConfig
) -> AppResult<()> {
    let plugin_file = fs
        ::canonicalize(path)
        .map_err(|error|
            cli_error(format!("cannot resolve database plugin '{}': {error}", path.display()))
        )?;
    if !plugin_file.is_file() {
        return Err(
            Box::new(
                cli_error(format!("database plugin path is not a file: {}", plugin_file.display()))
            )
        );
    }
    if
        live_database_state()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
    {
        return Err(
            Box::new(
                cli_error("a live database plugin is already configured in this test-host process")
            )
        );
    }
    let process_executor = process_config
        ::from_environment(executor == ExecutorArg::Process)
        .map_err(cli_error)?;
    let host = Arc::new(
        PluginHostInstance::new(PluginHostConfig {
            plugin_root: plugin_file
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf(),
            runtime,
            executor: executor.host_selection(),
            process_executor,
        })
    );
    let plugin_id = host
        .load_plugin_file(&plugin_file)
        .map_err(|error|
            cli_error(
                format!(
                    "failed to load live database DataSource plugin '{}': {error}",
                    plugin_file.display()
                )
            )
        )?;
    if !host.has_database_plugin() {
        let _ = host.unload_plugin(plugin_id);
        return Err(
            Box::new(
                cli_error(
                    format!(
                        "plugin '{}' does not declare a DataSource manifest",
                        plugin_file.display()
                    )
                )
            )
        );
    }
    if let Err(error) = host.check_database_readiness() {
        let cleanup = host.unload_plugin(plugin_id);
        return Err(
            Box::new(
                cli_error(match cleanup {
                    Ok(()) =>
                        format!("live database plugin failed SELECT 1 readiness check: {error}"),
                    Err(cleanup_error) =>
                        format!(
                            "live database plugin failed readiness check: {error}; unload also failed: {cleanup_error}"
                        ),
                })
            )
        );
    }
    let state = LiveDatabaseState {
        host,
        plugin_id,
        calls: Arc::new(Mutex::new(Vec::new())),
        next_call: Arc::new(AtomicUsize::new(0)),
    };
    let mut slot = live_database_state()
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if slot.is_some() {
        let _ = state.host.unload_plugin(state.plugin_id);
        return Err(Box::new(cli_error("a live database plugin was configured concurrently")));
    }
    *slot = Some(state);
    Ok(())
}

fn stop_live_database_plugin() -> Value {
    let state = live_database_state()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take();
    let Some(state) = state else {
        return json!({"success": true, "skipped": true});
    };
    match state.host.unload_plugin(state.plugin_id) {
        Ok(()) => json!({"success": true, "plugin_id": state.plugin_id}),
        Err(error) =>
            json!({"success": false, "plugin_id": state.plugin_id, "error": error.to_string()}),
    }
}

fn add_database_lifecycle_result(report: &mut Value, cleanup: Value) {
    let success = cleanup["success"].as_bool().unwrap_or(false);
    let test =
        json!({
        "name": "database.lifecycle.unload",
        "phase": "lifecycle",
        "passed": success,
        "duration_ms": 0,
        "outcome": cleanup,
        "checks": [check_json("database_unload_success", success, if success { "live DataSource plugin unloaded".to_owned() } else { "live DataSource plugin failed to unload".to_owned() })],
    });
    if let Some(tests) = report["tests"].as_array_mut() {
        tests.push(test);
    }
    if let Some(summary) = report["summary"].as_object_mut() {
        let total = summary.get("total").and_then(Value::as_u64).unwrap_or(0);
        let passed = summary.get("passed").and_then(Value::as_u64).unwrap_or(0);
        let failed = summary.get("failed").and_then(Value::as_u64).unwrap_or(0);
        summary.insert("total".to_owned(), Value::from(total + 1));
        if success {
            summary.insert("passed".to_owned(), Value::from(passed + 1));
        } else {
            summary.insert("failed".to_owned(), Value::from(failed + 1));
        }
    }
    report["database_source_cleanup"] = cleanup;
}

unsafe extern "C" fn live_database_execute(
    request: AbiSlice,
    cancellation: PluginCancellationV2,
    response_out: *mut AbiBuffer
) -> i32 {
    if response_out.is_null() {
        return PluginAbi::PLUGIN_STATUS_SERIALIZATION;
    }
    unsafe {
        *response_out = AbiBuffer::EMPTY;
    }

    let cancelled = unsafe { cancellation.is_cancelled() };
    if cancelled {
        return PluginAbi::PLUGIN_STATUS_CANCELLED;
    }

    let request_vec = unsafe { request.to_vec() };
    let Some(request_bytes) = request_vec else {
        return PluginAbi::PLUGIN_STATUS_SERIALIZATION;
    };
    let database_request = match serde_json::from_slice::<DatabaseRequest>(&request_bytes) {
        Ok(request) => request,
        Err(error) => {
            let result: Result<DatabaseResponse, DatabaseError> = Err(
                DatabaseError::new(
                    DatabaseErrorKind::InvalidRequest,
                    format!("live database bridge could not decode request: {error}")
                )
            );
            return write_database_response(response_out, &result);
        }
    };
    let (operation, statement) = request_operation_and_statement(&database_request);
    let request_json = serde_json::to_value(&database_request).unwrap_or(Value::Null);
    let (host, calls, call_number) = {
        let slot = live_database_state()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let Some(state) = slot.as_ref() else {
            let result: Result<DatabaseResponse, DatabaseError> = Err(
                DatabaseError::new(
                    DatabaseErrorKind::Connection,
                    "live database DataSource is not configured"
                )
            );
            return write_database_response(response_out, &result);
        };
        (
            Arc::clone(&state.host),
            Arc::clone(&state.calls),
            state.next_call.fetch_add(1, Ordering::Relaxed) + 1,
        )
    };

    match host.execute_database_raw(request_bytes, cancellation) {
        Ok(response_bytes) => {
            calls
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(
                    json!({
                "call_number": call_number,
                "operation": operation,
                "statement": statement,
                "request": request_json,
                "outcome": {"ok": true, "response_bytes": response_bytes.len()},
            })
                );
            unsafe {
                *response_out = AbiBuffer::from_vec(response_bytes);
            }
            PluginAbi::PLUGIN_STATUS_OK
        }
        Err(error) => {
            calls
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(
                    json!({
                "call_number": call_number,
                "operation": operation,
                "statement": statement,
                "request": request_json,
                "outcome": {"ok": false, "error": {"kind": error.kind, "message": error.message}},
            })
                );
            let response: Result<DatabaseResponse, DatabaseError> = Err(error);
            write_database_response(response_out, &response)
        }
    }
}

// ------------------------------ Scenario loading / output helpers ------------------------------

fn read_database_fixtures(path: &Path) -> AppResult<DatabaseFixtures> {
    let bytes = fs
        ::read(path)
        .map_err(|error|
            cli_error(format!("cannot read database fixtures '{}': {error}", path.display()))
        )?;
    let fixtures: DatabaseFixtures = serde_json
        ::from_slice(&bytes)
        .map_err(|error|
            cli_error(format!("invalid database fixture JSON '{}': {error}", path.display()))
        )?;
    fixtures.validate()?;
    Ok(fixtures)
}

fn cli_error(message: impl Into<String>) -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, message.into())
}

fn print_help() {
    println!(
        r#"EZF Plugin Test Host (ABI V2)

USAGE:
  ezf-plugin-test-host inspect <plugin-library> [options]
  ezf-plugin-test-host call   <plugin-library> --request <request.json> [options]
  ezf-plugin-test-host test   <plugin-library> --scenario <scenario.json> [options]

OPTIONS:
  --executor in-process|process  Select the existing PluginHost executor (default: in-process)
  --request, -r <file>           JSON request for 'call'
  --scenario, -s <file>          JSON integration-test scenario for 'test'
  --database-fixtures <file>     JSON database mock rules for inspect/call, or override scenario fixtures
  --database-plugin <file>       Live DataSource library; requires scenario database.mode=live
  --timeout-ms <n>               Override plugin call timeout (1..600000 ms)
  --unload-timeout-ms <n>        Override runtime quiescing timeout
  --lifecycle-timeout-ms <n>     Override on_unload/destroy callback timeout
  --output, -o <file>             Also write the machine-readable JSON output to a file
  --format json                   JSON is the only supported output format

Process mode:
  Build both binaries: cargo build -p PluginTestHost -p PluginWorker
  The worker is resolved beside the test-host executable, or via EZF_PLUGIN_WORKER_PATH.
  On Linux, configure EZF_PLUGIN_SANDBOX_PROFILE=domain_plugin (or the relevant enforced
  profile) or explicitly opt into development mode with EZF_PLUGIN_ALLOW_UNCONFINED=1.
  The test host never silently disables the process executor's sandbox checks.

A test scenario contains 'tests', optional 'timeouts', 'executor', 'principal', and 'database'.
  database.mode defaults to 'mock'; use 'live' with --database-plugin to forward to a real DataSource plugin.
A case may include a 'request', 'session_closed': true, and an 'expect' object. Results are JSON;
exit code 0 means all cases and lifecycle checks passed, 1 means a test failed, 2 means CLI/setup error.
"#
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use DatabaseContracts::{
        DatabaseColumn,
        DatabaseRow,
        DatabaseType,
        DatabaseValue,
        QueryRequest,
        ResultColumn,
    };

    #[test]
    fn parses_http_methods_case_insensitively() {
        assert_eq!(parse_method("GET").unwrap(), PluginHttpMethod::Get);
        assert_eq!(parse_method("post").unwrap(), PluginHttpMethod::Post);
        assert!(parse_method("TRACE").is_err());
    }

    #[test]
    fn live_variant_filter_scenario_uses_actual_endpoint_path_and_database_mode() {
        let suite: TestSuite = serde_json
            ::from_str(
                r#"{
            "name":"VariantFilter live DB",
            "executor":"process",
            "database":{"mode":"live"},
            "tests":[{"name":"load SNV","request":{"endpoint_id":"load-snv-data","method":"POST","path":"/loadSnvData","body":{"patient_ids":["00000000-0000-0000-0000-000000000001"]}},"expect":{"status":200,"database_call_count":1}}]
        }"#
            )
            .unwrap();
        assert_eq!(suite.database.mode.as_deref(), Some("live"));
        let request = suite.tests[0].request.clone().unwrap().into_endpoint_request().unwrap();
        assert_eq!(request.endpoint_id, "load-snv-data");
        assert_eq!(request.method, PluginHttpMethod::Post);
        assert_eq!(request.raw_path, "/loadSnvData");
    }

    #[test]
    fn json_body_path_equality_supports_nested_json() {
        let body = json!({"status":"loaded","patients":[{"id":"p1"}]});
        let loaded = json!("loaded");
        let patient_id = json!("p1");
        assert_eq!(json_path_value(&body, "status"), Some(&loaded));
        assert_eq!(json_path_value(&body, "patients.0.id"), Some(&patient_id));
        assert_eq!(json_path_value(&body, "patients.1.id"), None);
    }

    #[test]
    fn validates_live_database_mode_without_mock_rules() {
        let fixtures: DatabaseFixtures = serde_json::from_str(r#"{"mode":"live"}"#).unwrap();
        assert!(fixtures.validate().is_ok());
        let mixed: DatabaseFixtures = serde_json
            ::from_str(r#"{"mode":"live","default_execute_rows_affected":2}"#)
            .unwrap();
        assert!(mixed.validate().is_err());
    }

    #[test]
    fn error_contains_can_match_plugin_http_error_bodies_and_host_timeouts() {
        let http_error =
            json!({"kind":"response","status":400,"body":{"code":"invalid_input","message":"invalid limit"}});
        let timeout_error = json!({"kind":"error","status":504,"message":"plugin call timeout"});
        assert!(endpoint_error_text_for_assertion(&http_error).unwrap().contains("invalid limit"));
        assert!(endpoint_error_text_for_assertion(&timeout_error).unwrap().contains("timeout"));
        assert!(
            endpoint_error_text_for_assertion(
                &json!({"kind":"response","status":200,"body":{"ok":true}})
            ).is_none()
        );
    }

    #[test]
    fn request_json_uses_test_principal_and_serializes_body() {
        let input: JsonRequest = serde_json
            ::from_str(
                r#"{"endpoint_id":"create","method":"POST","path":"/items","body":{"name":"example"}}"#
            )
            .unwrap();
        let request = input.into_endpoint_request().unwrap();
        assert_eq!(request.method, PluginHttpMethod::Post);
        assert_eq!(request.raw_path, "/items");
        assert_eq!(request.content_type.as_deref(), Some("application/json"));
        assert_eq!(
            serde_json::from_slice::<Value>(&request.body).unwrap(),
            json!({"name":"example"})
        );
        assert_eq!(request.principal.username, "test-user");
    }

    #[test]
    fn database_rules_match_operation_statement_and_call_number() {
        let fixtures = DatabaseFixtures {
            rules: vec![DatabaseFixtureRule {
                operation: Some("query".to_owned()),
                statement_contains: Some("FROM patient".to_owned()),
                call_number: Some(2),
                response: Some(
                    DatabaseResponse::Query(ResultSet {
                        rows: vec![DatabaseRow {
                            columns: vec![DatabaseColumn {
                                name: "id".to_owned(),
                                value: DatabaseValue::I32(7),
                            }],
                        }],
                    })
                ),
                error: None,
            }],
            ..DatabaseFixtures::default()
        };
        let request = DatabaseRequest::Query(QueryRequest {
            statement: "SELECT id FROM patients".to_owned(),
            parameters: Vec::new(),
            columns: vec![ResultColumn::new("id", DatabaseType::I32)],
        });
        assert!(
            matches!(resolve_database_fixture(&fixtures, &request, 1), Ok(DatabaseResponse::Query(ResultSet { rows })) if rows.is_empty())
        );
        assert!(
            matches!(resolve_database_fixture(&fixtures, &request, 2), Ok(DatabaseResponse::Query(ResultSet { rows })) if rows.len() == 1)
        );
    }

    #[test]
    fn database_fixture_validation_rejects_ambiguous_rules() {
        let fixtures = DatabaseFixtures {
            rules: vec![DatabaseFixtureRule {
                operation: Some("query".to_owned()),
                ..DatabaseFixtureRule::default()
            }],
            ..DatabaseFixtures::default()
        };
        assert!(fixtures.validate().is_err());
    }

    #[test]
    fn database_rule_can_return_a_deterministic_contract_error() {
        let fixtures = DatabaseFixtures {
            rules: vec![DatabaseFixtureRule {
                operation: Some("execute".to_owned()),
                statement_contains: Some("FORCE_TEST_DB_ERROR".to_owned()),
                call_number: None,
                response: None,
                error: Some(DatabaseFixtureError {
                    kind: DatabaseErrorKind::Query,
                    message: "expected fixture error".to_owned(),
                }),
            }],
            ..DatabaseFixtures::default()
        };
        let request = DatabaseRequest::Execute(DatabaseContracts::ExecuteRequest {
            statement: "UPDATE x SET y = 1 /* FORCE_TEST_DB_ERROR */".to_owned(),
            parameters: Vec::new(),
        });
        let error = resolve_database_fixture(&fixtures, &request, 1).unwrap_err();
        assert_eq!(error.kind, DatabaseErrorKind::Query);
        assert_eq!(error.message, "expected fixture error");
    }

    #[test]
    fn integration_scenario_deserializes_process_timeout_and_session_context() {
        let suite: TestSuite = serde_json
            ::from_str(
                r#"{
            "name":"process timeout",
            "executor":"process",
            "timeouts":{"call_ms":150},
            "principal":{"user_id":"u1","username":"tester","given_name":"Test","family_name":"User","groups":["qa"],"session_id":"s1"},
            "tests":[{"name":"slow","request":{"endpoint_id":"slow","method":"GET","path":"/slow"},"expect":{"error_contains":"timeout"}},
                     {"name":"close session","session_closed":true,"session_id":"s1","expect":{"session_close_success":true}}]
        }"#
            )
            .unwrap();
        assert_eq!(suite.executor.as_deref(), Some("process"));
        assert_eq!(suite.timeouts.call_ms, Some(150));
        assert_eq!(suite.tests.len(), 2);
        assert_eq!(suite.principal.as_ref().unwrap().session_id, "s1");
        suite.timeouts.validate().unwrap();
    }

    #[test]
    fn parses_process_executor_and_scenario_timeout_overrides() {
        assert_eq!(parse_executor("process").unwrap(), ExecutorArg::Process);
        let mut options = CliOptions::default();
        options.timeout_ms = Some(125);
        let config = runtime_config(
            &(PluginTimeouts { call_ms: Some(500), unload_ms: None, lifecycle_ms: None }),
            &options
        );
        assert_eq!(config.call_timeout, Duration::from_millis(125));
    }
}
