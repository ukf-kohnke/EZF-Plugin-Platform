# Factory singleton migration and explicit PluginHost ownership

## Target ownership model

`BackendSrv` is the composition root for the production plugin runtime. It translates deployment settings into a typed `PluginHostConfig`, creates one `Arc<PluginHostInstance>`, and passes that same handle to startup, CLI operations, Infrastructure services, `UserApi::AppState`, request dispatch, session cleanup, readiness checks, and shutdown.

```rust
let config = plugin_host_config::from_environment()?;
let plugin_host = Arc::new(PluginHostInstance::new(config));

prepare_startup(plugin_host.as_ref())?;
let user_service = Infrastructure::UserIdentity::build_service_with_host(
    Arc::clone(&plugin_host),
);
let app_state = AppState::from_shared_config_with_plugin_host(
    Arc::new(main_config),
    user_service,
    plugin_host,
);
```

The snippet illustrates the flow inside the BackendSrv binary; `plugin_host_config` is an application-private composition-root module, not a library API.

## Runtime API boundary

The runtime-owned public types live under `PluginHost::PluginHostInstance`:

- `PluginManagerError`, `PluginLoadPolicy`, `PluginLoadReport`, `PluginLoadFailure`, and `PluginRequirement`;
- `PluginExecutorSelection`, `PluginExecutorKind`, `PluginHealthState`, and `PluginHealthSnapshot`;
- `PluginHostConfig`, `PluginRuntimeConfig`, `ProcessExecutorConfig`, `SandboxConfig`, `SandboxProfile`, and `WorkerEnvironment`.

Runtime implementation modules import these types from the runtime modules, never through `crate::Factory`. The old `PluginHost::Factory` namespace re-exports the public types to preserve source compatibility for existing callers.

`PluginRuntimeConfig` itself no longer reads environment variables. Backend deployment settings are translated in `BackendSrv::plugin_host_config`; the remaining environment parser in `Factory.rs` exists only for callers of the legacy process-wide facade.

## Compatibility boundary

The `PluginHost::Factory` functions and `OnceLock<Arc<PluginHostInstance>>` remain available for older integrations. They are not used by production BackendSrv startup, CLI, REST dispatch, readiness, session notification, or shutdown paths.

Deprecated compatibility builders remain for older callers and tests:

- `AppState::new` and `AppState::from_shared_config`;
- `Infrastructure::UserIdentity::build_service`;
- `Infrastructure::DataTransfer::build_service`;
- no-host `Infrastructure::Datasource::DatabaseExecutor` helpers.

New code should accept `Arc<PluginHostInstance>` or `&PluginHostInstance` and use the corresponding `*_with_host` APIs. The compatibility wrappers can be removed in a later breaking release after downstream callers have migrated.

## Validation

The source changes were statically checked for:

- no `crate::Factory` / `super::Factory` references in active `PluginHostInstance` or `PluginManager` runtime modules;
- no `PluginHost::Factory` calls in `BackendSrv` production sources;
- no direct process-environment reads in `PluginRuntimeConfig` or the active ProcessExecutor implementation;
- explicit host injection through BackendSrv, Infrastructure production adapters, and UserApi runtime paths;
- patch whitespace and clean-apply validation.

A Rust build and test run must still be performed in a Rust toolchain environment. This working environment did not provide `cargo` or `rustc`.
