//! Internal Factory catalog operations.
use super::*;

pub fn plugin_catalog() -> PluginCatalog {
    instance().plugin_catalog()
}
pub fn plugin_manifests() -> Vec<PluginManifest> {
    instance().plugin_manifests()
}
pub fn plugin_names_for_endpoints(
    endpoints: &[(PluginContracts::ApiContract::PluginHttpMethod, &str)]
) -> Vec<Option<String>> {
    instance().plugin_names_for_endpoints(endpoints)
}

impl PluginHost {
    pub fn plugin_catalog(&self) -> PluginCatalog {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .catalog()
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
