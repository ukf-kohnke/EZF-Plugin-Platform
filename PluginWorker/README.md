# ezf-plugin-worker (Phase 4/5 development worker)

This binary is used by the internal `ProcessExecutor` for real child-process integration. It uses
`PluginIpc` framing, loads ABI V2 libraries through `PluginExecutionCore`, serves all `PluginExecutor`
operations over IPC, and participates in the host-owned process lifecycle and timeout escalation.

## Explicit development-only invocation

The worker requires both an absolute Unix-domain socket path and an explicit opt-in flag:

```bash
ezf-plugin-worker --ipc-socket /absolute/path/to/private.sock --allow-unconfined
```

The supervisor/test harness owns and binds the socket. The worker connects to it; the IPC stream is
not multiplexed onto stdout. The host sends `Hello`, `Initialize` with profile
`DevelopmentUnconfined`, then `LoadPlugin`. The worker responds with `Hello`, `SandboxReady`, and
`Initialized`. It accepts `Invoke`, `HealthProbe`, and `Shutdown` messages after loading.

## Current limits

- **Not a production sandbox.** The worker reports `sandbox_enforced = false` and explicitly lists
  that OS sandboxing is not implemented. It refuses startup unless `--allow-unconfined` is passed.
- **Unix-domain sockets only.** Windows transport is not implemented.
- **Single-flight.** Plugin callbacks execute serially. The host sends `Cancel` while a callback is
  in flight; this worker cannot process that frame until the callback returns, so the host kills
  and reaps the entire worker after its bounded cancellation grace period if the callback does not
  finish. There is no automatic retry.
- **No capability broker yet.** Host-provided database capability callbacks are not forwarded across
  IPC. The worker can execute a datasource plugin's explicit `ExecuteDatabase` operation, but
  plugins that rely on the host-services database callback are not ready for process mode.
- **Opt-in internal ProcessExecutor integration.** The public factory API is unchanged and remains
  in-process by default. For local tests, set `EZF_PLUGIN_EXECUTOR=process`,
  `EZF_PLUGIN_ALLOW_UNCONFINED=1`, and `EZF_PLUGIN_WORKER_PATH` to the absolute worker binary path.

Integration tests build the existing `PluginHost/tests/pluginhost_integration_fixture` SDK plugin
as a separate `cdylib`. They exercise both the protocol directly and the unchanged Factory API through
`ProcessExecutor`, including `ping`, graceful unload/destroy, process reaping, and timeout escalation
against an endpoint configured to ignore cancellation. All tests run unconfined and are not sandbox
acceptance tests.

## Integration test

From the backend workspace root, run:

```bash
cargo test -p PluginWorker
```

The tests compile the fixture cdylib in separate temporary target directories, launch the worker
binary, perform real endpoint invocations, verify graceful shutdown, and verify that an uncooperative
call is force-terminated after the cancellation grace period. They need Rust and Cargo.
