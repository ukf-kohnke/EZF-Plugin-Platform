# Explicit PluginHostInstance integration

The production BackendSrv composition root owns one `Arc<PluginHostInstance>` and shares it with
startup, CLI operations, `Infrastructure` adapters, and `UserApi::AppState`.

```rust
// BackendSrv translates deployment settings into a typed PluginHostConfig at its composition root.
let config = build_plugin_host_config_from_deployment()?;
let plugin_host = Arc::new(PluginHostInstance::new(config));

Infrastructure::MainSetup::prepare_startup(plugin_host.as_ref())?;
let user_service = Infrastructure::UserIdentity::build_service_with_host(Arc::clone(&plugin_host));
let app_state = AppState::from_shared_config_with_plugin_host(
    Arc::new(main_config),
    user_service,
    Arc::clone(&plugin_host),
);
```

The backend-side `plugin_host_config` module translates `EZF_PLUGIN_*` deployment settings into
`PluginHostConfig`, `PluginRuntimeConfig`, and `ProcessExecutorConfig`. The runtime receives only
those typed values and does not depend on `ApplicationConfig` or read the backend's worker/sandbox
environment conventions during execution.

The `PluginHost::Factory` module remains as a compatibility facade backed by one
process-wide `OnceLock<Arc<PluginHostInstance>>`. Its shared-instance accessor and no-host
compatibility builders are deprecated. New application code must not use it. The runtime
implementation and its error/executor types are defined and exported by `PluginHostInstance`; the
Factory facade only re-exports those public types while preserving historical module-level calls.

Compatibility builders remain available for older callers and tests:

- `AppState::new` and `AppState::from_shared_config`
- `Infrastructure::UserIdentity::build_service`
- `Infrastructure::DataTransfer::build_service`
- The no-host `DatabaseExecutor` helpers

Production BackendSrv code uses the explicit `*_with_host` builders and carries the same host
instance through request dispatch, database-capability routing, session cleanup, readiness, and
shutdown.
