//! Deprecated compatibility accessors for plugins.
use super::*;

pub fn with_plugin_by_name<R, F>(plugin_name: &str, func: F) -> Option<R>
    where F: FnOnce(&dyn IPlugin) -> R
{
    instance().with_plugin_by_name(plugin_name, func)
}

pub fn with_all_plugins<R, F>(func: F) -> Option<Vec<R>> where F: Fn(&dyn IPlugin) -> R {
    instance().with_all_plugins(func)
}

impl PluginHost {
    pub fn with_plugin_by_name<R, F>(&self, plugin_name: &str, func: F) -> Option<R>
        where F: FnOnce(&dyn IPlugin) -> R
    {
        if plugin_name.is_empty() {
            return None;
        }
        let runtime = {
            let manager = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let id = manager
                .plugin_ids()
                .into_iter()
                .find(|id|
                    manager
                        .get_manifest(*id)
                        .is_some_and(|manifest| manifest.name.eq_ignore_ascii_case(plugin_name))
                )?;
            manager.get_runtime(id)?
        };
        let proxy = PluginProxy::new(runtime);
        Some(func(&proxy))
    }

    pub fn with_all_plugins<R, F>(&self, func: F) -> Option<Vec<R>> where F: Fn(&dyn IPlugin) -> R {
        let runtimes = {
            let manager = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let ids = manager.plugin_ids();
            if ids.is_empty() {
                return None;
            }
            ids.into_iter()
                .filter_map(|id| manager.get_runtime(id))
                .collect::<Vec<_>>()
        };
        Some(
            runtimes
                .into_iter()
                .map(|runtime| {
                    let proxy = PluginProxy::new(runtime);
                    func(&proxy)
                })
                .collect()
        )
    }
}
