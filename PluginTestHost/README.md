# EZF minimal plugin test host

`ezf-plugin-test-host` is a small standalone CLI for testing one ABI V2 plugin without starting
`BackendSrv`. It creates a fresh `PluginHostInstance`, uses the In-Process executor, supplies a
deterministic mock for the database capability, and unloads the plugin before exit.

## Build

From the EZF-Backend workspace root:

```bash
cargo build -p PluginTestHost
```

The executable is `target/debug/ezf-plugin-test-host` (Windows uses `.exe`).

## Inspect a plugin

```bash
cargo run -p PluginTestHost -- inspect ./path/to/libmy_plugin.so
```

`inspect` loads one exact library file, lets the existing PluginHost / PluginExecutionCore perform
ABI V2 and manifest validation, prints plugin metadata and endpoints, and unloads the library.
It does not scan the plugin directory.

## Execute a JSON request

```bash
cargo run -p PluginTestHost -- call ./path/to/libmy_plugin.so \
  --request PluginTestHost/examples/request.json
```

Request-file schema:

```json
{
  "endpoint_id": "create",
  "method": "POST",
  "path": "/example/items",
  "base_path": "/",
  "raw_query": "dry_run=true",
  "content_type": "application/json",
  "body": { "name": "example" },
  "principal": {
    "user_id": "test-user",
    "username": "test-user",
    "given_name": "Plugin",
    "family_name": "Test",
    "groups": ["plugin-test"],
    "session_id": "plugin-test-session"
  }
}
```

Only `endpoint_id`, `method`, and `path` are required. `method` accepts GET, POST, PUT, PATCH or
DELETE (case-insensitive). `body` is a JSON value that is serialized to request-body bytes. If no
principal is supplied, a deterministic test principal is used. `query` is accepted as an alias for
`raw_query`; `raw_path` is accepted as an alias for `path`.

The CLI dispatches through `PluginHostInstance::handle_loaded_endpoint`, which never discovers or
loads other plugin libraries. Missing endpoint IDs or paths therefore fail without scanning sibling
plugins. The selected plugin is unloaded regardless of whether inspection or request execution
succeeds; unload failures are reported.

## Database mock

The mock is intentionally minimal and deterministic:

- database query requests return an empty result set;
- database execute requests return `rows_affected: 1`;
- malformed database request payloads return a contract-level `InvalidRequest` error;
- cancellation is propagated as the ABI cancellation status.

This is enough for exercising endpoint code paths without a live database, but it does not emulate
SQL semantics or validate real schema/query behavior. Use a separate integration test and an isolated
test database for those cases.

ABI V2 does not provide a user-data pointer for the database callback. Because this CLI intentionally
runs one plugin and one host per process, the mock callback is stateless and uses no global mutable
mock configuration. Process execution is intentionally not exposed in Phase 1.
