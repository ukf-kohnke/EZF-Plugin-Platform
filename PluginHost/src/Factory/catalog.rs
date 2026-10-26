//! Internal Factory catalog operations.
use super::*;

pub fn plugin_catalog() -> PluginCatalog {
    let singleton = instance();
    let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    manager.catalog()
}

/// Returns plugin manifests without cloning the catalog's separate endpoint-binding list.
/// Use this for API responses that expose manifests but do not need dispatch metadata.
pub fn plugin_manifests() -> Vec<PluginManifest> {
    let singleton = instance();
    let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    manager.plugin_manifests()
}

/// Resolves multiple endpoint owners under one manager lock. Intended for aggregate health
/// checks such as readiness, where resolving the required route set one-by-one would add
/// lock traffic and cloning the complete catalog would copy unrelated endpoint metadata.
pub fn plugin_names_for_endpoints(
    endpoints: &[(PluginContracts::ApiContract::PluginHttpMethod, &str)]
) -> Vec<Option<String>> {
    let normalized_endpoints = endpoints
        .iter()
        .map(|(method, path)| { (*method, normalize_plugin_path(path).ok()) })
        .collect::<Vec<_>>();
    let singleton = instance();
    let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    normalized_endpoints
        .into_iter()
        .map(|(method, path)| {
            path.and_then(|normalized_path|
                manager
                    .endpoint_route_normalized(method, normalized_path)
                    .map(|route| route.plugin_name.as_ref().to_owned())
            )
        })
        .collect()
}
