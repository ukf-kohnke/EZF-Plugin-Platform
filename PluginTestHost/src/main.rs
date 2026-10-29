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
    PluginEndpointRequest,
    PluginEndpointResponse,
    PluginHttpMethod,
    PluginManifest,
    PluginPrincipal,
};
use PluginHost::PluginHostInstance::{
    PluginExecutorSelection,
    PluginHostConfig,
    PluginHostInstance,
    PluginRuntimeConfig,
};
use serde::Deserialize;
use serde_json::{ json, Value };
use std::{ env, error::Error, fs, io::{ self, ErrorKind }, path::{ Path, PathBuf }, process };

type AppResult<T> = Result<T, Box<dyn Error>>;

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        process::exit(2);
    }
}

fn run() -> AppResult<()> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        print_help();
        return Ok(());
    };
    if matches!(command.as_str(), "--help" | "-h" | "help") {
        print_help();
        return Ok(());
    }

    let plugin_file = args.next().ok_or_else(|| cli_error("missing plugin library path"))?;
    let plugin_file = PathBuf::from(plugin_file);
    let plugin_file = fs
        ::canonicalize(&plugin_file)
        .map_err(|error| {
            cli_error(format!("cannot resolve plugin library '{}': {error}", plugin_file.display()))
        })?;
    if !plugin_file.is_file() {
        return Err(
            Box::new(cli_error(format!("plugin path is not a file: {}", plugin_file.display())))
        );
    }

    match command.as_str() {
        "inspect" => {
            if args.next().is_some() {
                return Err(Box::new(cli_error("inspect accepts only a plugin library path")));
            }
            run_with_plugin(&plugin_file, |_host, plugin_id, manifest| {
                print_manifest(plugin_id, manifest)?;
                Ok(())
            })
        }
        "call" => {
            let mut request_path: Option<PathBuf> = None;
            while let Some(argument) = args.next() {
                match argument.as_str() {
                    "--request" | "-r" => {
                        if request_path.is_some() {
                            return Err(Box::new(cli_error("--request may be specified only once")));
                        }
                        request_path = Some(
                            PathBuf::from(
                                args
                                    .next()
                                    .ok_or_else(||
                                        cli_error("--request requires a JSON file path")
                                    )?
                            )
                        );
                    }
                    "--help" | "-h" => {
                        print_help();
                        return Ok(());
                    }
                    other => {
                        return Err(Box::new(cli_error(format!("unknown call argument: {other}"))));
                    }
                }
            }
            let request_path = request_path.ok_or_else(||
                cli_error("call requires --request <request.json>")
            )?;
            let request_bytes = fs
                ::read(&request_path)
                .map_err(|error| {
                    cli_error(
                        format!("cannot read request file '{}': {error}", request_path.display())
                    )
                })?;
            let input: JsonRequest = serde_json
                ::from_slice(&request_bytes)
                .map_err(|error| {
                    cli_error(format!("invalid request JSON '{}': {error}", request_path.display()))
                })?;
            run_with_plugin(&plugin_file, |host, plugin_id, manifest| {
                let request = input.into_endpoint_request()?;
                let method = request.method;
                let path = request.raw_path.clone();
                let endpoint_id = request.endpoint_id.clone();
                let response = host
                    .handle_loaded_endpoint(request)
                    .map_err(|error| {
                        cli_error(format!("plugin endpoint call failed: {error}"))
                    })?;
                print_call_result(plugin_id, manifest, &endpoint_id, method, &path, response)?;
                Ok(())
            })
        }
        other => Err(Box::new(cli_error(format!("unknown command '{other}'")))),
    }
}

fn run_with_plugin<F>(plugin_file: &Path, operation: F) -> AppResult<()>
    where F: FnOnce(&PluginHostInstance, i64, &PluginManifest) -> AppResult<()>
{
    let parent = plugin_file.parent().unwrap_or_else(|| Path::new("."));
    let host = PluginHostInstance::new(PluginHostConfig {
        plugin_root: parent.to_path_buf(),
        runtime: PluginRuntimeConfig::default(),
        executor: PluginExecutorSelection::InProcess,
    });

    // ABI V2 has no per-callback user-data pointer. This deterministic mock is deliberately
    // compiled into the single-process test CLI and is shared only by its one host invocation.
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
    let manifests = host.plugin_manifests();
    let manifest = match manifests.as_slice() {
        [manifest] => manifest.clone(),
        other => {
            let unload_result = host.unload_plugin(plugin_id);
            let detail = format!("expected exactly one loaded manifest, found {}", other.len());
            unload_result.map_err(|error| cli_error(format!("{detail}; cleanup failed: {error}")))?;
            return Err(Box::new(cli_error(detail)));
        }
    };

    let operation_result = operation(&host, plugin_id, &manifest);
    let unload_result = host
        .unload_plugin(plugin_id)
        .map_err(|error| {
            cli_error(format!("plugin call/inspection completed but unload failed: {error}"))
        });
    operation_result?;
    unload_result?;
    Ok(())
}

#[derive(Debug, Deserialize)]
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
    /// The request body is a JSON value; the test host serializes it to JSON bytes for the plugin.
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
                        format!("unsupported HTTP method '{}'; use GET, POST, PUT, PATCH, or DELETE", value)
                    )
                )
            ),
    }
}

fn print_manifest(plugin_id: i64, manifest: &PluginManifest) -> AppResult<()> {
    let endpoints = manifest.endpoints
        .iter()
        .map(|endpoint| {
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
        })
        .collect::<Vec<_>>();
    let output =
        json!({
        "loaded": true,
        "plugin_id": plugin_id,
        "abi": "V2",
        "contract_version": manifest.contract_version,
        "name": manifest.name,
        "version": manifest.version,
        "kind": manifest.kind,
        "endpoints": endpoints,
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn print_call_result(
    plugin_id: i64,
    manifest: &PluginManifest,
    endpoint_id: &str,
    method: PluginHttpMethod,
    path: &str,
    response: PluginEndpointResponse
) -> AppResult<()> {
    let body = if response.body.is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice::<Value>(&response.body) {
            Ok(value) => value,
            Err(_) =>
                match String::from_utf8(response.body.clone()) {
                    Ok(text) => Value::String(text),
                    Err(_) => json!({ "non_utf8_body_bytes": response.body.len() }),
                }
        }
    };
    let endpoints = manifest.endpoints
        .iter()
        .map(
            |endpoint|
                json!({
        "id": endpoint.id,
        "method": endpoint.method.as_str(),
        "path": endpoint.path,
    })
        )
        .collect::<Vec<_>>();
    let output =
        json!({
        "plugin": { "id": plugin_id, "name": manifest.name, "version": manifest.version, "contract_version": manifest.contract_version, "abi": "V2" },
        "available_endpoints": endpoints,
        "request": { "endpoint_id": endpoint_id, "method": method.as_str(), "path": path },
        "response": {
            "status": response.status,
            "content_type": response.content_type,
            "headers": response.headers,
            "body": body,
        }
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn cli_error(message: impl Into<String>) -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, message.into())
}

fn print_help() {
    println!(
        r#"EZF minimal plugin test host (In-Process, ABI V2)

USAGE:
  ezf-plugin-test-host inspect <plugin-library>
  ezf-plugin-test-host call <plugin-library> --request <request.json>

COMMANDS:
  inspect  Load one plugin, validate ABI/manifest, and list its endpoints.
  call     Load one plugin, execute the request described by JSON, print the response.

The database capability is a deterministic mock: queries return an empty result set;
execute requests report one affected row. The plugin is unloaded before the command exits.

Request JSON example:
  {{
    "endpoint_id": "ping",
    "method": "GET",
    "path": "/example/ping"
  }}

For a POST request, add a JSON value in "body". If omitted, a test principal is used.
"#
    );
}

/// Deterministic database mock for the one host created by this CLI. Query operations return
/// no rows; execute operations report one affected row. Malformed payloads become contract-level
/// database errors rather than panics across the ABI.
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

    let cancelled = unsafe { cancellation.is_cancelled() };
    if cancelled {
        return PluginAbi::PLUGIN_STATUS_CANCELLED;
    }

    let result_vec = unsafe { request.to_vec() };
    let Some(request_bytes) = result_vec else {
        return PluginAbi::PLUGIN_STATUS_SERIALIZATION;
    };
    let database_request = match serde_json::from_slice::<DatabaseRequest>(&request_bytes) {
        Ok(request) => request,
        Err(error) => {
            let response: Result<DatabaseResponse, DatabaseError> = Err(
                DatabaseError::new(
                    DatabaseErrorKind::InvalidRequest,
                    format!("test-host database mock could not decode request: {error}")
                )
            );
            return write_database_response(response_out, &response);
        }
    };

    let response: Result<DatabaseResponse, DatabaseError> = match database_request {
        DatabaseRequest::Query(_) => Ok(DatabaseResponse::Query(ResultSet::default())),
        DatabaseRequest::Execute(_) =>
            Ok(DatabaseResponse::Execute(ExecutionResult { rows_affected: 1 })),
    };
    write_database_response(response_out, &response)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_http_methods_case_insensitively() {
        assert_eq!(parse_method("GET").unwrap(), PluginHttpMethod::Get);
        assert_eq!(parse_method("post").unwrap(), PluginHttpMethod::Post);
        assert!(parse_method("TRACE").is_err());
    }

    #[test]
    fn request_json_uses_a_test_principal_and_serializes_body() {
        let input: JsonRequest = serde_json
            ::from_str(
                r#"{
            "endpoint_id":"create",
            "method":"POST",
            "path":"/items",
            "body":{"name":"example"}
        }"#
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

    unsafe extern "C" fn never_cancelled(_context: *const std::ffi::c_void) -> bool {
        false
    }

    fn invoke_mock(request: &DatabaseRequest) -> Result<DatabaseResponse, DatabaseError> {
        let request_bytes = serde_json::to_vec(request).unwrap();
        let cancellation = PluginCancellationV2 {
            context: std::ptr::null(),
            is_cancelled: never_cancelled,
        };
        let mut output = AbiBuffer::EMPTY;
        let status = unsafe {
            mock_database_execute(AbiSlice::from_bytes(&request_bytes), cancellation, &mut output)
        };
        assert_eq!(status, PluginAbi::PLUGIN_STATUS_OK);
        let bytes = (unsafe { output.to_vec() }).unwrap();
        unsafe {
            mock_database_free(output);
        }
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn db_mock_returns_empty_queries_and_one_affected_row_for_execute() {
        let query = DatabaseRequest::Query(DatabaseContracts::QueryRequest {
            statement: "SELECT 1".to_owned(),
            parameters: Vec::new(),
            columns: Vec::new(),
        });
        assert_eq!(invoke_mock(&query).unwrap(), DatabaseResponse::Query(ResultSet::default()));

        let execute = DatabaseRequest::Execute(DatabaseContracts::ExecuteRequest {
            statement: "UPDATE example SET value = ?".to_owned(),
            parameters: Vec::new(),
        });
        assert_eq!(
            invoke_mock(&execute).unwrap(),
            DatabaseResponse::Execute(ExecutionResult { rows_affected: 1 })
        );
    }
}
