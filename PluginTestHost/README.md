# EZF Plugin Test Host — Phase 2

`ezf-plugin-test-host` tests one ABI V2 domain plugin without starting `BackendSrv`. It uses the same
`PluginHostInstance`, `PluginExecutionCore`, and (in process mode) `PluginWorker`/`PluginIpc`
implementation as the backend. A suite loads one selected domain library, runs scenarios sequentially,
records host and database diagnostics, notifies sessions when configured, and records the final
unload as a lifecycle test. Database access can either use deterministic mock fixtures or an explicitly
selected real DataSource plugin (for example the deployed `MolgenDB` plugin).

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
- `database`: database mode and, in mock mode, deterministic database fixtures. Omit `mode` or use
  `"mode": "mock"` for fixtures. Use `"mode": "live"` to route database requests to a real DataSource plugin.
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

## Live SQL Server / MolgenDB mode

To test against the same SQL Server database configuration used by a running backend, the test host loads
its deployed `MolgenDB` DataSource plugin separately. The DLL/dylib must remain next to the same
`config.json` and `database.ini` used by the backend. The scenario contains no credentials or
connection strings; pass the deployed DataSource library path explicitly:

```json
"database": { "mode": "live" }
```

For the macOS debug deployment included with EZF-Backend, run from the workspace root:

```bash
cargo build -p PluginTestHost -p PluginWorker
EZF_PLUGIN_ALLOW_UNCONFINED=1 cargo run -p PluginTestHost -- test \
  ./deployment/debug/plugins/libVariantFilter.dylib \
  --scenario PluginTestHost/examples/scenario-variantfilter-live-db-smoke.json \
  --database-plugin ./deployment/debug/plugins/database/MolgenDB/libMolgenDB.dylib \
  --executor process \
  --output target/variantfilter-live-db-report.json
```

Use `EZF_PLUGIN_ALLOW_UNCONFINED=1` only for local development when the required enforced process
sandbox profile is not configured. If a sandbox profile is configured, omit that variable. Adjust the
library filenames/extensions to the platform and deployment that you actually run. Importantly,
`--database-plugin` must point to the deployed library, not just `target/debug/libMolgenDB.dylib`, unless
the build output directory also contains the matching `config.json` and `database.ini`.

The test host performs a readiness probe (`SELECT 1`) and forwards the Domain plugin's database
requests through the DataSource plugin using the existing ABI V2 path. It unloads the DataSource after
the Domain plugin test suite. This is a **separate connection pool** from the already running backend;
it targets the same SQL Server/database because it uses the same deployed MolgenDB configuration. It
does not borrow the backend process's live SQL connection or pool.

`database.mode = "live"` cannot be mixed with `--database-fixtures` or mock fixture rules. VariantFilter's
endpoint identifier is `load-snv-data`, but its actual HTTP method/path are `POST /loadSnvData` (the path
is not `/load-snv-data`). The request body must contain one or more unique patient UUIDs:

```json
{
  "endpoint_id": "load-snv-data",
  "method": "POST",
  "path": "/loadSnvData",
  "body": { "patient_ids": ["REPLACE-WITH-A-REAL-PATIENT-UUID"] }
}
```

Two ready-to-use scenarios are provided:

- `examples/scenario-variantfilter-live-db-smoke.json` uses a valid example UUID and checks that the live SQL request is executed and the endpoint returns HTTP 200. It can return `status: "not_loaded"` if the UUID has no analysis, so it is useful as a connection/query smoke test without knowing a real patient ID.
- `examples/scenario-variantfilter-load-snv-data.json` performs the stricter check that the response status is `loaded`, then calls `GET /getSnvCache` in the same session and verifies that the cache is available. Replace the example UUID in this scenario with a real patient ID from the test database which has a selected analysis containing SNV variants; otherwise the assertion is expected to fail.

To find candidate IDs, execute `examples/variantfilter-find-patient-with-snvs.sql` against the database configured for MolgenDB, then copy one `patient_id` into the strict scenario's `patient_ids` array. The SQL deliberately mirrors VariantFilter's current `TOP (1) ... ORDER BY a.id` analysis selection. Use only a test-safe database/account. Although this endpoint issues a read query, it writes the loaded result into the plugin's in-memory session cache until session closure/unload.

## Machine-readable results and exit codes

The `test` command always emits a JSON report (`schema_version: 1`) with suite configuration,
setup, per-case assertions, DB requests/outcomes, runtime health, unload outcome, and a summary.
`--output <path>` writes the same JSON document to disk. Exit codes:

- `0`: every case and final lifecycle check passed.
- `1`: one or more test assertions or lifecycle expectations failed.
- `2`: invalid CLI/configuration input or an inspect/call setup error.

For CI, the JSON report is the stable result interface; ordinary stdout diagnostics are not used by
the test host.
