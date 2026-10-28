//! Internal Factory loading operations.
use super::*;

#[allow(unused)]
pub fn unload_plugin(id: i64) -> Result<(), PluginManagerError> {
    // Keep the plugin registered for the complete unload operation. `PluginRuntime::unload`
    // transitions its health to `Stopping` before waiting for in-flight calls to drain.
    // Keeping the registry entry visible makes that transient state observable through the
    // public readiness/health APIs and prevents an active unload from looking like an empty,
    // ready host. The operation guard still prevents a concurrent load/reload of the same
    // canonical library path.
    let canonical_path = {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        manager.plugins
            .get(&id)
            .map(|item| item.path.clone())
            .ok_or(PluginManagerError::PluginNotFound { id })?
    };
    let _operation_guard = begin_plugin_operation(&canonical_path, PluginOperationKind::Unloading)?;

    // Re-check membership after acquiring the operation guard because another lifecycle
    // operation may have completed between the initial lookup and reservation. Only the
    // runtime Arc is borrowed here; the registry entry itself stays in place until unload
    // succeeds, so health snapshots can expose `Stopping`/failure states during the operation.
    let runtime = {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        manager.get_runtime(id).ok_or(PluginManagerError::PluginNotFound { id })?
    };

    let started = std::time::Instant::now();
    let outcome = runtime.unload();
    if !outcome.completed() {
        emit_log(log::LevelFilter::Error, &format!("Plugin unload failed or timed out: id={}", id));
        return Err(PluginManagerError::PluginUnloadFailed { id });
    }

    let elapsed = started.elapsed();
    let removed_item = {
        let singleton = instance();
        let mut manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        manager.detach_plugin(id)?
    };
    drop(removed_item);

    match outcome {
        PluginUnloadOutcome::Unloaded => {
            emit_log(
                log::LevelFilter::Info,
                &format!("Plugin unloaded gracefully: id={} after {:?}", id, elapsed)
            );
            Ok(())
        }
        PluginUnloadOutcome::ForcedTermination => {
            emit_log(
                log::LevelFilter::Error,
                &format!(
                    "Plugin unload ended non-gracefully; worker process reaped and registry entry removed: id={} after {:?}",
                    id,
                    elapsed
                )
            );
            Err(PluginManagerError::PluginUnloadFailed { id })
        }
        PluginUnloadOutcome::Failed => Err(PluginManagerError::PluginUnloadFailed { id }),
    }
}

pub fn is_plugin_loaded(plugin_name: &str, plugin_path: &str) -> Result<bool, PluginManagerError> {
    if find_id_by_name(plugin_name).is_some() {
        return Ok(true);
    }

    let report = load_plugins_with_policy(plugin_path, PluginLoadPolicy::optional_all())?;
    Ok(report.loaded_count() > 0 && find_id_by_name(plugin_name).is_some())
}

pub(crate) fn find_id_by_name(plugin_name: &str) -> Option<i64> {
    let singleton = instance();
    let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

    manager
        .plugin_ids()
        .into_iter()
        .find(|id|
            manager
                .get_manifest(*id)
                .is_some_and(|manifest| manifest.name.eq_ignore_ascii_case(plugin_name))
        )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PluginOperationKind {
    Loading,
    Unloading,
}

static PLUGIN_OPERATIONS_IN_PROGRESS: OnceLock<
    Mutex<BTreeMap<PathBuf, PluginOperationKind>>
> = OnceLock::new();

fn plugin_operations_in_progress() -> &'static Mutex<BTreeMap<PathBuf, PluginOperationKind>> {
    PLUGIN_OPERATIONS_IN_PROGRESS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

struct PluginOperationGuard {
    path: PathBuf,
}

impl Drop for PluginOperationGuard {
    fn drop(&mut self) {
        let mut paths = plugin_operations_in_progress()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        paths.remove(&self.path);
    }
}

fn begin_plugin_operation(
    path: &Path,
    kind: PluginOperationKind
) -> Result<PluginOperationGuard, PluginManagerError> {
    let canonical_path = PluginManager::normalize_path(path);
    let mut paths = plugin_operations_in_progress()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if paths.contains_key(&canonical_path) {
        return Err(PluginManagerError::PluginLoadInProgress { path: canonical_path });
    }
    paths.insert(canonical_path.clone(), kind);
    Ok(PluginOperationGuard { path: canonical_path })
}

/// Emit the selected execution backend and current child PID after a plugin is registered.
/// This log is independent of EZF_API_TIMING so executor selection is visible in normal startup logs.
fn log_plugin_executor_snapshot(plugin_name: &str, path: &Path) {
    match plugin_health_by_name(plugin_name) {
        Some(snapshot) => {
            let worker_pid = snapshot.worker_pid
                .map(|pid| pid.to_string())
                .unwrap_or_else(|| "none".to_owned());
            emit_log(
                log::LevelFilter::Info,
                &format!(
                    "PLUGIN_EXECUTOR component={:?} executor={} worker_pid={} state={:?} path={:?}",
                    snapshot.name,
                    snapshot.executor.as_str(),
                    worker_pid,
                    snapshot.state,
                    path.display().to_string()
                )
            );
        }
        None =>
            emit_log(
                log::LevelFilter::Warn,
                &format!(
                    "PLUGIN_EXECUTOR component={:?} executor=unknown worker_pid=none state=unknown path={:?} diagnostic=health_snapshot_unavailable",
                    plugin_name,
                    path.display().to_string()
                )
            ),
    }
}

pub(super) fn plugin_operation_kind(path: &Path) -> Option<PluginOperationKind> {
    let canonical_path = PluginManager::normalize_path(path);
    plugin_operations_in_progress()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&canonical_path)
        .copied()
}

pub fn load_plugin_file(path: &Path) -> Result<i64, PluginManagerError> {
    load_plugin_file_with_config(path, PluginRuntimeConfig::from_environment())
}

pub fn load_plugin_file_with_config(
    path: &Path,
    config: PluginRuntimeConfig
) -> Result<i64, PluginManagerError> {
    load_plugin_file_inner(path, config)
}

fn load_plugin_file_inner(
    path: &Path,
    config: PluginRuntimeConfig
) -> Result<i64, PluginManagerError> {
    let canonical_path = PluginManager::normalize_path(path);

    // Reserve the canonical library path before inspecting the registry. This closes the
    // unload/load race where an unload could remove the registry entry between the initial
    // duplicate check and acquisition of the lifecycle operation guard.
    let _operation_guard = begin_plugin_operation(&canonical_path, PluginOperationKind::Loading)?;

    {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing_id) = manager.paths.get(&canonical_path).copied() {
            return Err(PluginManagerError::PluginAlreadyLoaded {
                path: canonical_path,
                id: existing_id,
            });
        }
    }

    let loaded = PluginManager::load_plugin(&canonical_path, config)?;
    let plugin_manifest = loaded.manifest().clone();

    let result = {
        let singleton = instance();
        let mut manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(existing_id) = manager.paths.get(&canonical_path).copied() {
            Err(PluginManagerError::PluginAlreadyLoaded {
                path: canonical_path.clone(),
                id: existing_id,
            })
        } else {
            match manager.catalog_with_manifest(&plugin_manifest) {
                Ok(catalog) => Ok(manager.insert_loaded_plugin(&canonical_path, loaded, catalog)),
                Err(error) => Err(error),
            }
        }
    };

    if let Ok(id) = result.as_ref() {
        emit_log(
            log::LevelFilter::Info,
            &format!(
                "Plugin loaded successfully: '{}' (id={}) from '{}'",
                plugin_manifest.name,
                id,
                canonical_path.display()
            )
        );
        log_plugin_executor_snapshot(&plugin_manifest.name, &canonical_path);
    }
    result
}

pub fn load_plugins(plugin_path: &str) -> Result<usize, PluginManagerError> {
    let report = load_plugins_with_policy_and_config(
        plugin_path,
        PluginLoadPolicy::required_all(),
        PluginRuntimeConfig::from_environment()
    )?;
    Ok(report.loaded_count())
}

pub fn load_plugins_with_policy(
    plugin_path: &str,
    policy: PluginLoadPolicy
) -> Result<PluginLoadReport, PluginManagerError> {
    load_plugins_with_policy_and_config(
        plugin_path,
        policy,
        PluginRuntimeConfig::from_environment()
    )
}

pub fn load_plugins_with_policy_and_config(
    plugin_path: &str,
    policy: PluginLoadPolicy,
    config: PluginRuntimeConfig
) -> Result<PluginLoadReport, PluginManagerError> {
    let entries = PluginManager::discover_plugin_paths(plugin_path)?;
    let mut report = PluginLoadReport {
        requested_path: PathBuf::from(plugin_path),
        ..PluginLoadReport::default()
    };

    for entry in entries {
        let canonical_path = match entry {
            Ok(path) => path,
            Err(error) => {
                report.failures.push(PluginLoadFailure {
                    path: PathBuf::from(plugin_path),
                    plugin_name: None,
                    requirement: policy.default_requirement,
                    error,
                });
                continue;
            }
        };
        report.discovered.push(canonical_path.clone());

        let path_requirement = policy.requirement_for_path(&canonical_path);
        // Reserve the path before checking the registry for the same reason as the direct
        // file-load path: a concurrent unload must not be able to create a reload window.
        let _operation_guard = match
            begin_plugin_operation(&canonical_path, PluginOperationKind::Loading)
        {
            Ok(guard) => guard,
            Err(error) => {
                report.failures.push(PluginLoadFailure {
                    path: canonical_path,
                    plugin_name: None,
                    requirement: path_requirement,
                    error: error.to_string(),
                });
                continue;
            }
        };

        {
            let singleton = instance();
            let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if manager.paths.contains_key(&canonical_path) {
                report.skipped_already_loaded.push(canonical_path);
                continue;
            }
        }

        let loaded = match PluginManager::load_plugin(&canonical_path, config) {
            Ok(loaded) => loaded,
            Err(error) => {
                let error_text = error.to_string();
                report.failures.push(PluginLoadFailure {
                    path: canonical_path,
                    plugin_name: None,
                    requirement: path_requirement,
                    error: error_text,
                });
                continue;
            }
        };

        let manifest = loaded.manifest().clone();
        let manifest_requirement = policy.requirement_for_name(&manifest.name);
        let requirement = if
            path_requirement == PluginRequirement::Required ||
            manifest_requirement == PluginRequirement::Required
        {
            PluginRequirement::Required
        } else {
            PluginRequirement::Optional
        };
        let plugin_name = manifest.name.clone();

        let registration = {
            let singleton = instance();
            let mut manager = singleton.inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            if let Some(existing_id) = manager.paths.get(&canonical_path).copied() {
                Err(PluginManagerError::PluginAlreadyLoaded {
                    path: canonical_path.clone(),
                    id: existing_id,
                })
            } else {
                match manager.catalog_with_manifest(&manifest) {
                    Ok(catalog) =>
                        Ok(manager.insert_loaded_plugin(&canonical_path, loaded, catalog)),
                    Err(error) => Err(error),
                }
            }
        };

        match registration {
            Ok(_) => {
                report.loaded.push(plugin_name.clone());
                log_plugin_executor_snapshot(&plugin_name, &canonical_path);
            }
            Err(error) =>
                report.failures.push(PluginLoadFailure {
                    path: canonical_path,
                    plugin_name: Some(plugin_name),
                    requirement,
                    error: error.to_string(),
                }),
        }
    }

    let loaded_names = loaded_plugin_names();
    for (configured_name, requirement) in &policy.overrides {
        if *requirement != PluginRequirement::Required {
            continue;
        }

        let present =
            loaded_names
                .iter()
                .any(|loaded_name| normalize_plugin_name(loaded_name) == *configured_name) ||
            report.loaded
                .iter()
                .any(|loaded_name| normalize_plugin_name(loaded_name) == *configured_name);

        let matching_failure_index = report.failures.iter().position(|failure| {
            failure.plugin_name
                .as_deref()
                .is_some_and(|name| normalize_plugin_name(name) == *configured_name) ||
                failure.path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .is_some_and(|stem| normalize_plugin_name(stem) == *configured_name)
        });

        // A required plugin may fail before its manifest can be read (for example,
        // when the DLL is not a valid dynamic library). In that case we still want
        // the load report to identify the failed required plugin when the configured
        // name unambiguously matches the failing file, without creating a duplicate
        // synthetic failure for the same root cause.
        if let Some(index) = matching_failure_index {
            if report.failures[index].plugin_name.is_none() {
                report.failures[index].plugin_name = Some(configured_name.clone());
            }
        }

        let already_reported_for_name = matching_failure_index.is_some();

        if !present && !already_reported_for_name {
            report.failures.push(PluginLoadFailure {
                path: PathBuf::from(plugin_path),
                plugin_name: Some(configured_name.clone()),
                requirement: PluginRequirement::Required,
                error: format!("required plugin '{}' was not discovered or loaded", configured_name),
            });
        }
    }

    // A required plugin load failure already represents the missing/unusable plugin.
    // Do not add a second synthetic "not discovered or loaded" failure for the same
    // configured name; that would double-count one root cause and make the load report
    // misleading.
    if report.discovered.is_empty() && report.failures.is_empty() {
        emit_log(
            log::LevelFilter::Warn,
            &format!("No plugin libraries were found in the configured directory '{}'.", plugin_path)
        );
    }

    for failure in &report.failures {
        let level = match failure.requirement {
            PluginRequirement::Required => log::LevelFilter::Error,
            PluginRequirement::Optional => log::LevelFilter::Warn,
        };
        emit_log(
            level,
            &format!(
                "PLUGIN_LOAD_FAILURE requirement={} plugin={:?} path={:?} error={:?}",
                match failure.requirement {
                    PluginRequirement::Required => "required",
                    PluginRequirement::Optional => "optional",
                },
                failure.plugin_name.as_deref().unwrap_or("unknown"),
                failure.path.display().to_string(),
                failure.error
            )
        );
    }

    emit_log(
        if report.has_required_failures() {
            log::LevelFilter::Error
        } else {
            log::LevelFilter::Info
        },
        &report.summary()
    );

    if report.has_required_failures() {
        return Err(PluginManagerError::PluginLoadPolicyViolation { report });
    }

    Ok(report)
}

pub fn load_plugin_if_missing(
    plugin_name: &str,
    plugin_path: &str
) -> Result<bool, PluginManagerError> {
    is_plugin_loaded(plugin_name, plugin_path)
}
