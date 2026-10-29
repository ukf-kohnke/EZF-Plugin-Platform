//! Internal Factory sessions operations.
use super::*;

pub fn notify_session_closed(context: &PluginSessionContext) -> PluginSessionNotificationReport {
    instance().notify_session_closed(context)
}

impl PluginHost {
    pub fn notify_session_closed(
        &self,
        context: &PluginSessionContext
    ) -> PluginSessionNotificationReport {
        let runtimes = {
            let manager = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            manager
                .plugin_ids()
                .into_iter()
                .filter_map(|id| {
                    Some((manager.get_manifest(id)?.name.clone(), manager.get_runtime(id)?))
                })
                .collect::<Vec<_>>()
        };
        let mut report = PluginSessionNotificationReport::default();
        for (plugin_name, runtime) in runtimes {
            let notification = catch_unwind(
                AssertUnwindSafe(|| {
                    PluginProxy::new(runtime).on_session_closed(
                        context,
                        &PluginCancellation::uncancelled()
                    )
                })
            );
            match notification {
                Ok(Ok(())) => report.notified_plugins.push(plugin_name),
                Ok(Err(error)) => {
                    report.notified_plugins.push(plugin_name.clone());
                    report.failures.push(PluginSessionNotificationFailure { plugin_name, error });
                }
                Err(_) =>
                    report.failures.push(PluginSessionNotificationFailure {
                        plugin_name,
                        error: PluginLifecycleError::new(
                            "plugin_session_callback_panicked",
                            "The plugin panicked while processing a closed session"
                        ),
                    }),
            }
        }
        report
    }
}
