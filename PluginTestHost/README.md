# EZF Plugin Test Host — Phase 2

`ezf-plugin-test-host` tests one ABI V2 plugin without starting `BackendSrv`. It uses the same
`PluginHostInstance`, `PluginExecutionCore`, and (in process mode) `PluginWorker`/`PluginIpc`
implementation as the backend. A suite loads one selected library, runs scenarios sequentially,
records host and database diagnostics, notifies sessions when configured, and records the final
unload as a lifecycle test.

## Build

From the workspace root:

```bash
cargo build -p PluginTestHost -p PluginWorker
cargo test -p PluginTestHost
```

The worker binary is resolved beside `ezf-plugin-test-host`. For a nonstandard location, set
`EZF_PLUGIN_WORKER_PATH` to an absolute path to `ezf-plugin-worker`.

## Inspect and make a single request

```bash
cargo run -p PluginTestHost -- inspect ./target/debug/libmy_plugin.so
cargo run -p PluginTestHost -- call ./target/debug/libmy_plugin.so \
  --request PluginTestHost/examples/request.json --executor in-process
```

All command output is JSON. `--output report.json` additionally writes that JSON to a file.

## Process mode

```bash
cargo build -p PluginTestHost -p PluginWorker
# Local development only, when an enforced sandbox profile is not configured:
EZF_PLUGIN_ALLOW_UNCONFINED=1 cargo run -p PluginTestHost -- test \
  ./target/debug/libmy_plugin.so \
  --scenario PluginTestHost/examples/scenario.json \
  --executor process --output target/plugin-test-report.json
```

The test host never disables sandbox checks implicitly. On Linux, use a suitable configured
`EZF_PLUGIN_SANDBOX_PROFILE` (or per-plugin `EZF_PLUGIN_SANDBOX_PROFILES`) for enforced execution,
or explicitly opt into `EZF_PLUGIN_ALLOW_UNCONFINED=1` only for development/testing. The current
process executor supports Unix-domain sockets only. An unconfined worker is **not** OS-sandboxed.

## Scenario format

A suite is one JSON document with these top-level fields:

- `name`: optional report label.
- `executor`: `in_process` or `process`; CLI `--executor` overrides it.
- `timeouts`: optional `call_ms`, `unload_ms`, and `lifecycle_ms`. CLI timeout flags override these.
- `principal`: default session/user context for requests without a request-specific `principal`.
- `database`: deterministic database mock configuration.
- `tests`: nonempty ordered test case list.
- `expect_unload_success`: expected final unload result (default `true`).

Example:

```json
{
  "name": "domain plugin",
  "executor": "process",
  "timeouts": { "call_ms": 750, "unload_ms": 3000, "lifecycle_ms": 500 },
  "principal": {
    "user_id": "test-42", "username": "tester", "given_name": "Test",
    "family_name": "User", "groups": ["plugin-test"], "session_id": "session-42"
  },
  "database": {
    "default_query": { "rows": [] },
    "default_execute_rows_affected": 1,
    "rules": [
      {
        "operation": "query",
        "statement_contains": "FROM patients",
        "response": {
          "result": "query",
          "rows": [{ "columns": [
            { "name": "id", "value": { "type": "i32", "value": 42 } }
          ] }]
        }
      },
      {
        "operation": "execute",
        "statement_contains": "FORCE_TEST_DB_ERROR",
        "error": { "kind": "query", "message": "intentional failure" }
      }
    ]
  },
  "tests": [
    {
      "name": "list endpoint",
      "request": {
        "endpoint_id": "list_patients", "method": "GET", "path": "/patients"
      },
      "expect": { "status": 200, "database_call_count": 1 }
    },
    {
      "name": "bad request returns expected validation error",
      "request": {
        "endpoint_id": "list_patients", "method": "GET", "path": "/patients?limit=bad"
      },
      "expect": { "status": 400, "error_contains": "invalid" }
    },
    {
      "name": "notify plugin about session closure",
      "session_closed": true,
      "session_id": "session-42",
      "expect": { "session_close_success": true }
    }
  ],
  "expect_unload_success": true
}
```

Each test case may contain:

- `request`: an endpoint request. The request uses the Phase-1 shape (`endpoint_id`, `method`,
  `path`, optional `base_path`, `raw_query`, `content_type`, JSON `body`, and `principal`).
- `session_closed: true` (also accepted as `notify_session_closed`) to invoke the plugin session
  close callback; the `session_id` is explicit or taken from the request/suite principal.
- `expect`: optional assertions `status`, `body_equals`, `error_contains`, `session_close_success`,
  `health_state`, and `database_call_count`.

A request timeout remains a testable outcome rather than an automatic CLI failure: use `status`
and/or `error_contains` to assert the expected timeout error. The report includes the health snapshot,
including active/timed-out call counters, before unload.

## Controlled database mock

`database` accepts:

- `default_query`: a `DatabaseContracts::ResultSet` (default is empty rows).
- `default_execute_rows_affected`: default affected-row count (default `1`).
- `default_error`: optional `{ "kind": "query", "message": "..." }` returned when no rule matches.
- `rules`: ordered rules. Each rule must contain a match condition (`operation`, `statement_contains`,
  or `call_number`) and exactly one of `response` or `error`. Matching is case-sensitive for SQL
  substring; operation matching is case-insensitive. `call_number` is 1-based across the current
  suite. Response examples follow the serialized `DatabaseResponse` contract, such as
  `{ "result": "query", "rows": [] }` or `{ "result": "execute", "rows_affected": 2 }`.

The Database ABI V2 callback has no per-callback user-data parameter. Accordingly, this CLI holds
one explicitly configured mock state per test-host process and runs one suite against one plugin
sequentially. It is not a PluginHost or BackendSrv global; each CLI invocation is its own process.

## Machine-readable results and exit codes

The `test` command always emits a JSON report (`schema_version: 1`) with suite configuration,
setup, per-case assertions, DB requests/outcomes, runtime health, unload outcome, and a summary.
`--output <path>` writes the same JSON document to disk. Exit codes:

- `0`: every case and final lifecycle check passed.
- `1`: one or more test assertions or lifecycle expectations failed.
- `2`: invalid CLI/configuration input or an inspect/call setup error.

For CI, the JSON report is the stable result interface; ordinary stdout diagnostics are not used by
the test host.
