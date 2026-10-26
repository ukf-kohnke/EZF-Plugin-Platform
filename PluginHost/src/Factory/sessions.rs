//! Internal Factory sessions operations.
use super::*;

pub fn notify_session_closed(context: &PluginSessionContext) -> PluginSessionNotificationReport {
    let runtimes = {
        let singleton = instance();
        let manager = match singleton.inner.lock() {
            Ok(manager) => manager,
            Err(poisoned) => poisoned.into_inner(),
        };

        manager
            .plugin_ids()
            .into_iter()
            .filter_map(|id| {
                let name = manager.get_manifest(id)?.name.clone();
                let runtime = manager.get_runtime(id)?;
                Some((name, runtime))
            })
            .collect::<Vec<_>>()
    };

    let mut report = PluginSessionNotificationReport::default();

    for (plugin_name, runtime) in runtimes {
        let notification = catch_unwind(
            AssertUnwindSafe(|| {
                let proxy = PluginProxy::new(runtime.clone());
                proxy.on_session_closed(context, &PluginCancellation::uncancelled())
            })
        );

        match notification {
            Ok(Ok(())) => {
                report.notified_plugins.push(plugin_name);
            }
            Ok(Err(error)) => {
                report.notified_plugins.push(plugin_name.clone());
                report.failures.push(PluginSessionNotificationFailure {
                    plugin_name,
                    error,
                });
            }
            Err(_) => {
                report.failures.push(PluginSessionNotificationFailure {
                    plugin_name,
                    error: PluginLifecycleError::new(
                        "plugin_session_callback_panicked",
                        "The plugin panicked while processing a closed session"
                    ),
                });
            }
        }
    }

    report
}
