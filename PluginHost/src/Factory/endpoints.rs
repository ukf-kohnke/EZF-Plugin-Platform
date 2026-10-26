//! Internal Factory endpoints operations.
use super::*;

pub fn loaded_plugin_names() -> Vec<String> {
    let singleton = instance();
    let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

    manager
        .plugin_ids()
        .into_iter()
        .filter_map(|id| manager.get_manifest(id).map(|manifest| manifest.name.to_string()))
        .collect()
}

pub fn loaded_plugin_ids() -> Vec<i64> {
    let singleton = instance();
    let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    manager.plugin_ids()
}

pub fn plugin_count() -> usize {
    let singleton = instance();
    let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    manager.plugin_count()
}

pub fn has_plugins() -> bool {
    plugin_count() > 0
}

/// Executes a plugin REST endpoint without requiring the caller to know the concrete plugin name.
///
/// Endpoint ownership is resolved from a precomputed runtime index keyed by HTTP method and
/// normalized path. This prevents application code from coupling itself to implementation-specific plugin
/// names such as `ProcessTracker` and also avoids fragile name-derivation heuristics from REST
/// function names.
pub fn handle_endpoint(
    request: PluginEndpointRequest
) -> Result<PluginEndpointResponse, PluginEndpointError> {
    let endpoint_path = request_endpoint_path(&request.base_path, &request.raw_path);
    let route = match resolve_endpoint_route(request.method, &endpoint_path, &request.endpoint_id)? {
        Some(route) => route,
        None => {
            // Preserve lazy discovery for callers that reach the dispatcher before plugins
            // have been explicitly loaded. The second lookup uses the same prebuilt index.
            load_plugins_with_policy(&plugin_root_dir(), PluginLoadPolicy::optional_all()).map_err(
                |error|
                    PluginEndpointError::internal(
                        "plugin_discovery_failed",
                        "Plugin discovery failed while resolving the endpoint",
                        error.to_string()
                    )
            )?;

            resolve_endpoint_route(
                request.method,
                &endpoint_path,
                &request.endpoint_id
            )?.ok_or_else(||
                PluginEndpointError::not_found(
                    "plugin_endpoint_not_found",
                    "No loaded plugin implements the requested endpoint",
                    &format!("No plugin implements {} {}", request.method, endpoint_path)
                )
            )?
        }
    };

    // The route lookup clones the runtime Arc while holding the manager mutex. Actual plugin
    // code always runs after that mutex has been released.
    route.runtime.handle_endpoint(request)
}

fn request_endpoint_path(base_path: &str, raw_path: &str) -> String {
    let base = base_path.trim();
    let raw = if raw_path.trim().is_empty() { "/" } else { raw_path.trim() };

    if base.is_empty() || base == "/" {
        return raw.to_owned();
    }

    let base = format!("/{}", base.trim_matches('/'));
    if raw == base {
        return "/".to_owned();
    }

    raw.strip_prefix(&format!("{base}/"))
        .map(|path| format!("/{path}"))
        .unwrap_or_else(|| raw.to_owned())
}

fn resolve_endpoint_route(
    method: PluginContracts::ApiContract::PluginHttpMethod,
    path: &str,
    endpoint_id: &str
) -> Result<Option<PluginEndpointRoute>, PluginEndpointError> {
    // Normalize outside the manager lock so even the remaining per-request string work does
    // not lengthen the critical section shared by concurrent dispatches and lifecycle changes.
    let normalized_path = normalize_plugin_path(path).ok();
    let route = normalized_path.and_then(|normalized_path| {
        let singleton = instance();
        let manager = singleton.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        manager.endpoint_route_normalized(method, normalized_path)
    });

    let Some(route) = route else {
        return Ok(None);
    };

    if !route.endpoint_id.as_ref().eq_ignore_ascii_case(endpoint_id) {
        return Err(
            PluginEndpointError::internal(
                "plugin_endpoint_contract_mismatch",
                "The requested endpoint contract does not match the registered REST endpoint",
                format!(
                    "HTTP endpoint {} {} is registered as endpoint id '{}', not '{}'",
                    method,
                    path,
                    route.endpoint_id.as_ref(),
                    endpoint_id
                )
            )
        );
    }

    Ok(Some(route))
}
