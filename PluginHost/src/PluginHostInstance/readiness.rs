//! Internal Factory readiness operations.
use super::*;

pub(crate) fn plugin_health_by_name_for(host: &PluginHostInstance, plugin_name: &str) -> Option<PluginHealthSnapshot> {
    host.plugin_health_by_name(plugin_name)
}

impl PluginHostInstance {
    pub fn plugin_health(&self) -> Vec<PluginHealthSnapshot> {
        let manager = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        manager.plugin_ids().into_iter().filter_map(|id| {
            let item = manager.plugins.get(&id)?;
            let mut health = item.health();
            if super::loading::plugin_operation_kind_for_host(self, &item.path) == Some(PluginOperationKind::Unloading) {
                health.state = PluginHealthState::Stopping;
            }
            Some(health)
        }).collect()
    }

    pub fn plugin_health_by_name(&self, plugin_name: &str) -> Option<PluginHealthSnapshot> {
        let manager = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        manager.plugin_ids().into_iter().find_map(|id| {
            let item = manager.plugins.get(&id)?;
            if !item.manifest().name.eq_ignore_ascii_case(plugin_name) { return None; }
            let mut health = item.health();
            if super::loading::plugin_operation_kind_for_host(self, &item.path) == Some(PluginOperationKind::Unloading) {
                health.state = PluginHealthState::Stopping;
            }
            Some(health)
        })
    }

    pub fn check_readiness(&self) -> Result<(), String> {
        let degraded = self.plugin_health().into_iter().filter(|h| h.state != PluginHealthState::Ready)
            .map(|h| format!("{}={:?}", h.name, h.state)).collect::<Vec<_>>();
        if degraded.is_empty() { Ok(()) } else { Err(format!("plugin readiness check failed: {}", degraded.join(", "))) }
    }
}
