//! Factory and singleton access layer for runtime plugin management.
//!
//! This module provides a thread-safe, process-wide entry point to load, query,
//! execute, and unload plugins through an internal `PluginManager` guarded by
//! `Mutex` + `OnceLock`.
//!
//! # Responsibilities
//!
//! - Lazily initialize a single `PluginManager` instance.
//! - Load one plugin file or scan/load many plugin files from a directory pattern.
//! - Check whether a plugin is already loaded (by plugin runtime name).
//! - Resolve plugin IDs by name.
//! - Execute endpoint requests (`handle_endpoint`).
//! - Broadcast sanitized backend-session lifecycle events to loaded plugins.
//! - Expose inspection helpers (loaded names/ids, count, catalog, `has_plugins`).
//! - Unload a single plugin or all plugins.
//! - Expose bounded runtime execution, health snapshots, and a readiness gate.
//!
//! # Error model
//!
//! Plugin loading/unloading uses `PluginManagerError`, which captures:
//!
//! - catalog validation errors,
//! - invalid paths and glob patterns,
//! - dynamic library / constructor symbol loading failures,
//! - runtime/manifest name mismatches,
//! - initialization failures,
//! - already-loaded / not-found / unload failures.
//!
//! `PluginManagerError` implements `Display` and `std::error::Error`, forwarding
//! nested sources where available.
//!
//! Endpoint execution maps loading/lookup failures to `PluginEndpointError` with
//! stable error codes/messages suitable for API responses.
//!
//! # Concurrency notes
//!
//! The global mutex protects only plugin-manager state (registration, lookup, catalog and lifecycle
//! bookkeeping). Each loaded plugin has its own read/write runtime gate: normal ABI calls take a
//! shared read lock and can execute concurrently, while unload marks the runtime as quiescing,
//! rejects new calls, and takes the exclusive write lock to drain all in-flight calls before
//! lifecycle callbacks and instance destruction. A per-library lifecycle-operation guard also
//! prevents load/unload races for the same dynamic library path.
//!
//! # Behavior notes
//!
//! - `load_plugin_if_missing` is a deprecated compatibility API; prefer explicit catalog/health lookup.
//! - REST endpoint lookup is catalog-based: callers identify an endpoint by its HTTP contract,
//!   not by a concrete plugin implementation name.
//! - `with_plugin_by_name` and `with_all_plugins` are deprecated compatibility APIs; prefer the
//!   catalog/endpoint APIs. `with_database_plugin` remains the explicit capability boundary.
//! - Plugin call limits default to 30s call timeout, 10s unload timeout, 8 worker threads,
//!   32 admitted calls, and 24 queued jobs. The three capacity limits can be overridden with
//!   `EZF_PLUGIN_WORKER_THREADS`, `EZF_PLUGIN_MAX_CONCURRENT_CALLS`, and
//!   `EZF_PLUGIN_QUEUE_CAPACITY`, or supplied explicitly to the `*_with_config` load APIs.
//! - ABI V2 uses cooperative cancellation for plugin calls. A caller-side timeout sets the
//!   per-operation cancellation signal; plugin code must observe it at safe interruption points
//!   and return. The host never terminates a plugin thread or invalidates plugin memory.
//! - On unload, every admitted call receives the same cancellation request and the runtime waits
//!   for those calls to drain before `on_unload` and `destroy` are started. Lifecycle callbacks
//!   themselves also receive a cooperative cancellation token. In-process lifecycle timeouts keep
//!   the runtime registered until the callback actually returns. Process-backed callbacks instead
//!   get a bounded Cancel-Grace/Kill/Wait escalation; confirmed forced termination detaches the
//!   dead worker but is still reported as an unload error because plugin cleanup may be incomplete.
//!
//! # Return conventions
//!
//! - `is_plugin_loaded`: `Ok(true)` when a plugin is available.
//! - `with_*` helpers: `None` when no matching plugin (or no plugins) is present.
//! - bulk loading returns a [`Factory::PluginLoadReport`] from the policy-aware API.
#![allow(non_snake_case)]
mod PluginManager;

#[cfg(target_os = "linux")]
const LOGGERDEF: &str = "unix";
#[cfg(target_os = "macos")]
const LOGGERDEF: &str = "macos";
#[cfg(target_os = "windows")]
const LOGGERDEF: &str = "windows";

fn emit_log(level: log::LevelFilter, message: &str) {
    match level {
        log::LevelFilter::Error => log::error!(target: LOGGERDEF, "{}", message),
        log::LevelFilter::Warn => log::warn!(target: LOGGERDEF, "{}", message),
        log::LevelFilter::Info => log::info!(target: LOGGERDEF, "{}", message),
        log::LevelFilter::Debug => log::debug!(target: LOGGERDEF, "{}", message),
        log::LevelFilter::Trace => log::trace!(target: LOGGERDEF, "{}", message),
        log::LevelFilter::Off => {}
    }
}

pub mod Factory;

#[cfg(test)]
mod load_policy_tests {
    use super::Factory::{ PluginLoadPolicy, PluginRequirement };
    use std::path::Path;

    #[test]
    fn explicit_plugin_requirement_overrides_default_by_name_and_path() {
        let policy = PluginLoadPolicy::optional_all().require("ProcessTracker");
        assert_eq!(policy.requirement_for_name("processtracker"), PluginRequirement::Required);
        assert_eq!(
            policy.requirement_for_path(Path::new("/plugins/ProcessTracker.so")),
            PluginRequirement::Required
        );
        assert_eq!(
            policy.requirement_for_path(Path::new("/plugins/VariantFilter.so")),
            PluginRequirement::Optional
        );
    }
}

#[cfg(test)]
mod load_report_tests {
    use super::Factory::{ PluginLoadFailure, PluginLoadReport, PluginRequirement };
    use std::path::PathBuf;

    #[test]
    fn report_distinguishes_required_and_optional_failures() {
        let mut report = PluginLoadReport {
            requested_path: PathBuf::from("/plugins"),
            ..PluginLoadReport::default()
        };
        report.failures.push(PluginLoadFailure {
            path: PathBuf::from("/plugins/optional.so"),
            plugin_name: Some("Optional".to_owned()),
            requirement: PluginRequirement::Optional,
            error: "load failed".to_owned(),
        });
        assert!(report.is_success());

        report.failures.push(PluginLoadFailure {
            path: PathBuf::from("/plugins/required.so"),
            plugin_name: Some("Required".to_owned()),
            requirement: PluginRequirement::Required,
            error: "load failed".to_owned(),
        });
        assert!(!report.is_success());
        assert_eq!(report.required_failures().len(), 1);
    }
}
