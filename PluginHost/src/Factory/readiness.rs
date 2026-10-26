//! Internal Factory readiness operations.
use super::*;

/// Returns health/telemetry snapshots for all loaded plugins in deterministic plugin-id order.
pub fn plugin_health() -> Vec<PluginHealthSnapshot> {
    let singleton = instance();
    let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    manager
        .plugin_ids()
        .into_iter()
        .filter_map(|id| {
            let item = manager.plugins.get(&id)?;
            let mut health = item.health();
            if plugin_operation_kind(&item.path) == Some(PluginOperationKind::Unloading) {
                health.state = PluginHealthState::Stopping;
            }
            Some(health)
        })
        .collect()
}

/// Returns the health snapshot for a specific plugin using case-insensitive name matching.
pub fn plugin_health_by_name(plugin_name: &str) -> Option<PluginHealthSnapshot> {
    let singleton = instance();
    let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    manager
        .plugin_ids()
        .into_iter()
        .find_map(|id| {
            let item = manager.plugins.get(&id)?;
            if !item.manifest().name.eq_ignore_ascii_case(plugin_name) {
                return None;
            }

            let mut health = item.health();
            if plugin_operation_kind(&item.path) == Some(PluginOperationKind::Unloading) {
                health.state = PluginHealthState::Stopping;
            }
            Some(health)
        })
}

/// Returns an error unless every loaded plugin is currently ready. This can be used as a
/// startup/readiness gate after plugin discovery and before serving traffic.
pub fn check_readiness() -> Result<(), String> {
    let health = plugin_health();
    let degraded = health
        .iter()
        .filter(|snapshot| snapshot.state != PluginHealthState::Ready)
        .map(|snapshot| format!("{}={:?}", snapshot.name, snapshot.state))
        .collect::<Vec<_>>();
    if degraded.is_empty() {
        Ok(())
    } else {
        Err(format!("plugin readiness check failed: {}", degraded.join(", ")))
    }
}
