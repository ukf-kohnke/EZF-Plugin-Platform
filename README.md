# EZF Plugin Platform

[Deutsche Dokumentation](README_DE.md) · [Plugin SDK](Plugin-SDK/README.md) · [Plugin Test Host](PluginTestHost/README.md) · [Plugin Worker](PluginWorker/README.md)

`EZF-Plugin-Platform` is the reusable Rust workspace for hosting, executing, and testing EZF plugins. It contains the plugin runtime, ABI execution core, process-worker IPC protocol, worker executable, standalone test host, and shared filter contracts. It is not the EZF application backend: HTTP routing, authentication, application configuration, deployment layout, and domain-specific use cases remain in the consuming application and its plugins.

The nested `Plugin-SDK` Git submodule is an independent Cargo workspace and owns its own `README.md` and `README_DE.md`. This document covers the platform around that SDK; it intentionally does not replace or duplicate the SDK documentation.

## Current source baseline

| Item | Current source value |
|---|---|
| Platform workspace version | `1.0.3` |
| Rust edition | `2024` |
| Cargo resolver | `2` |
| Plugin ABI | ABI V2 (`InstantiateV2`, ABI version `2`) |
| Logical plugin contract | SDK contract `9`; host also accepts legacy contract `8` without `description` |
| Host/worker IPC | Protocol `1.0` |
| Default executor | `InProcess` |
| Worker package / binary | `PluginWorker` `1.0.1` / `ezf-plugin-worker` |
| Test-host package / binary | `PluginTestHost` / `ezf-plugin-test-host` |
| Nested SDK workspace version | `1.0.2` |

The platform package version, SDK package version, manifest contract version, and binary ABI version describe different compatibility boundaries. Do not increment or compare them as though they were one version.

## Workspace components

The root `Cargo.toml` includes the following workspace members. `Plugin-SDK` is explicitly excluded and is built/tested as its own workspace.

| Crate | Package version | Responsibility |
|---|---:|---|
| `PluginHost` | `1.0.3` | Explicitly configured host instance, plugin catalog/registry, endpoint index, loading policy, lifecycle, health, DataSource selection, and executor selection. |
| `PluginExecutionCore` | `1.0.3` | Shared native-library loading and ABI V2 calls, ABI layout checks, response-buffer ownership and freeing. |
| `PluginIpc` | `1.0.3` | Versioned IPC messages, bounded frame encoding/decoding, request correlation and capability protocol types. It does not spawn processes or load plugins. |
| `PluginWorker` | `1.0.1` | `ezf-plugin-worker` process that loads and invokes one plugin library and serves host-brokered capabilities. |
| `PluginTestHost` | `1.0.3` | `ezf-plugin-test-host` CLI for inspecting, invoking and scenario-testing a plugin outside `BackendSrv`. |
| `FilterContracts` | `1.0.3` | Shared, database-independent filtering, sorting, column and pagination data structures. |

### Component boundaries

- **`PluginHost`** owns one `PluginHostInstance`. The instance captures its plugin root, executor choice, runtime limits, DataSource selection, database capability callback, catalog, and lifecycle state. Separate instances do not share a process-global registry or database callback.
- **`PluginExecutionCore`** is the common ABI V2 dynamic-library execution layer used by the in-process host and worker. It owns the library for the plugin lifetime and handles plugin-owned response buffers according to the ABI contract.
- **`PluginIpc`** defines the worker wire protocol. The current protocol is version `1.0`; the fixed header is 36 bytes, the default control section is limited to 1 MiB, and the default payload section to 64 MiB.
- **`PluginWorker`** runs outside the backend process in process mode. It loads the plugin through `PluginExecutionCore` and brokers database/log requests through IPC. It does not expose HTTP routes and does not own the host's plugin catalog.
- **`PluginTestHost`** exercises the same host/runtime path from a CLI. It supports deterministic database fixtures or a separately loaded live `DataSource` plugin.
- **`FilterContracts`** is a neutral filter/query model shared by domain components. It is not part of `Plugin-SDK` and does not define the plugin ABI or database capability contract.

## Runtime and execution modes

Consumers construct a `PluginHostConfig` and pass it to `PluginHostInstance::new`. The host library deliberately does not read deployment environment variables or application-specific configuration. The application composition root (or a CLI adapter such as `PluginTestHost`) translates its own settings into the typed host configuration.

`PluginHostConfig` includes the plugin root, `PluginRuntimeConfig`, executor selection (`InProcess` or `Process`) and `ProcessExecutorConfig`.

| Mode | Execution | Current constraints |
|---|---|---|
| `InProcess` | Plugin library is loaded and invoked in the host process through `PluginExecutionCore`. | Plugin calls can run concurrently within the per-plugin worker/admission limits. A plugin panic/error is contained by the runtime boundary, but a native plugin still shares the host process. |
| `Process` | A worker process loads the plugin and exchanges requests with the host using `PluginIpc`. | The current transport uses Unix-domain sockets and is supported on Unix targets. A worker's top-level IPC invocation path is single-flight; increasing the host's in-process worker-thread count does not create parallel calls inside that worker. |

The default `PluginRuntimeConfig` is currently 30 seconds for endpoint calls, 10 seconds for unload, 10 seconds for lifecycle operations, 16 persistent worker threads, 32 admitted calls and queue capacity 24 per plugin. These are defaults, not universal sizing recommendations. The Process executor defaults to 500 ms cancellation grace and a 2 second worker shutdown timeout; both are bounded by validation.

A host selects database capability from loaded plugins with manifest kind `DataSource`. Consumers that require database access must make sure the host has exactly one selectable DataSource. Domain plugins use the SDK database capability and do not link to a concrete database plugin.

## Process worker and sandbox

Process mode launches `ezf-plugin-worker`. `ProcessExecutorConfig` carries the optional absolute worker-binary path, sandbox settings, explicit worker environment, cancellation grace and shutdown timeout. `PluginHost` does not infer these values from EZF-specific environment variables.

The Linux worker currently supports enforced profiles on Linux `x86_64` and `aarch64`:

- `DomainPlugin`: Landlock read-only filesystem allowlist, private writable temporary directory, seccomp restrictions, `no_new_privs`, capability checks, inherited-file-descriptor reduction and selected `RLIMIT` controls. Database access must go through the host capability broker.
- `DataSourcePlugin`: the same base restrictions plus Landlock TCP destination-port rules. Port rules are not an IP allowlist. Firewall/container/network policy must separately restrict destination IPs and non-TCP egress; the worker can verify only that the operator acknowledged this external control.
- `DevelopmentUnconfined`: an explicit development/test mode without an OS sandbox. It is not a production isolation boundary.

Enforced mode fails closed when the platform/kernel does not provide the required primitives. DataSource profiles require Landlock ABI 4 or later. The worker does not create cgroups or a user namespace, change UID, or enforce a total temporary-storage quota; those remain deployment responsibilities. See [Plugin Worker](PluginWorker/README.md) for the exact environment variables, limits, and supported controls.

## Standalone plugin test host

Build and run the CLI from the platform workspace:

```bash
cargo build -p PluginTestHost -p PluginWorker
cargo test -p PluginTestHost

# Inspect a compiled plugin library
cargo run -p PluginTestHost -- inspect /absolute/path/to/libmy_plugin.so

# Run a JSON scenario
cargo run -p PluginTestHost -- test /absolute/path/to/libmy_plugin.so \
  --scenario PluginTestHost/examples/scenario.json \
  --executor in-process \
  --output target/plugin-test-report.json
```

The CLI also supports a single `call`, process execution, deterministic database mock rules, and an explicitly selected live DataSource plugin. It emits a machine-readable JSON report and uses exit codes `0` (all assertions passed), `1` (test/lifecycle assertion failed), and `2` (invalid input or setup error). A mock state is scoped to one test-host process; a scenario suite is executed sequentially against one plugin instance. See [Plugin Test Host](PluginTestHost/README.md) for the complete scenario schema and live-database examples.

## Build and test

Run these commands from the root of this repository (`EZF-Plugin-Platform`):

```bash
# Build all platform workspace members
cargo build --workspace

# Run all workspace tests
cargo test --workspace

# Focus on host/runtime and worker integration
cargo test -p PluginHost
cargo test -p PluginWorker -- --test-threads=1
cargo test -p PluginIpc
cargo test -p PluginTestHost
```

`Plugin-SDK` is excluded from this workspace. Run its own build/tests from `Plugin-SDK/` according to its [English](Plugin-SDK/README.md) or [German](Plugin-SDK/README_DE.md) README. Platform builds that load a real plugin still require a plugin library built for the same operating system/architecture and compatible ABI V2 contract. Tests which use a live DataSource additionally require its plugin-local configuration and an available database.

## Relationship to EZF-Backend

In EZF-Backend this repository is checked out as the `Plugin-Platform` Git submodule. The backend root Cargo workspace excludes the platform root workspace but includes selected packages (notably `PluginWorker`) where needed by its own build. `PluginTestHost` remains a member of the separate platform workspace and should be built with `--manifest-path Plugin-Platform/Cargo.toml` when invoked from the backend root.

The backend supplies application concerns such as Rocket routes, authentication, `ApplicationConfig`, deployment environment parsing and the concrete `MolgenDB` DataSource. The platform supplies the reusable host/runtime, worker and test-host components. For backend-specific integration and the currently exposed API, consult the EZF-Backend repository README when this submodule is checked out inside EZF-Backend.

## Further documentation

- [Plugin Host process-executor configuration](PluginHost/docs/process-executor-configuration.md) (currently documented in German)
- [Plugin Worker](PluginWorker/README.md) · [German](PluginWorker/README_DE.md)
- [Plugin Test Host](PluginTestHost/README.md) · [German](PluginTestHost/README_DE.md)
- [Plugin SDK](Plugin-SDK/README.md) · [German](Plugin-SDK/README_DE.md)
