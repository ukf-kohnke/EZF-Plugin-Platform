//! Plugin catalog operations for one explicit host instance.
use super::*;

impl PluginHostInstance {
    pub fn plugin_catalog(&self) -> PluginCatalog {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .catalog()
    }

    /// Returns a cheap Arc clone of the cached API snapshot for loaded Domain plugins.
    /// A new immutable snapshot is published atomically with the route index on load/unload.
    pub fn active_domain_api_snapshot(&self) -> std::sync::Arc<ActiveDomainApiSnapshot> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .active_domain_api_snapshot()
    }

    pub fn plugin_manifests(&self) -> Vec<PluginManifest> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .plugin_manifests()
    }

    pub fn plugin_names_for_endpoints(
        &self,
        endpoints: &[(PluginContracts::ApiContract::PluginHttpMethod, &str)]
    ) -> Vec<Option<String>> {
        let normalized = endpoints
            .iter()
            .map(|(method, path)| (*method, normalize_plugin_path(path).ok()))
            .collect::<Vec<_>>();
        let manager = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        normalized
            .into_iter()
            .map(|(method, path)|
                path.and_then(|path|
                    manager
                        .endpoint_route_normalized(method, path)
                        .map(|route| route.plugin_name.as_ref().to_owned())
                )
            )
            .collect()
    }
}
