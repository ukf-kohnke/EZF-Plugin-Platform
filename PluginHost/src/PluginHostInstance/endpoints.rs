//! Internal Factory endpoint and registry operations.
use super::*;

pub(crate) fn loaded_plugin_names_for(host: &PluginHostInstance) -> Vec<String> {
    host.loaded_plugin_names()
}

impl PluginHostInstance {
    pub fn loaded_plugin_names(&self) -> Vec<String> {
        let manager = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        manager
            .plugin_ids()
            .into_iter()
            .filter_map(|id| manager.get_manifest(id).map(|manifest| manifest.name.clone()))
            .collect()
    }

    pub fn loaded_plugin_ids(&self) -> Vec<i64> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .plugin_ids()
    }

    pub fn plugin_count(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .plugin_count()
    }

    pub fn has_plugins(&self) -> bool {
        self.plugin_count() > 0
    }

    /// Dispatches against this host's catalog and runtime registry only. For the legacy
    /// backend this retains the existing fallback to configured plugin discovery.
    pub fn handle_endpoint(
        &self,
        request: PluginEndpointRequest
    ) -> Result<PluginEndpointResponse, PluginEndpointError> {
        let endpoint_path = request_endpoint_path(&request.base_path, &request.raw_path);
        let route = match
            resolve_endpoint_route_for(self, request.method, &endpoint_path, &request.endpoint_id)?
        {
            Some(route) => route,
            None => {
                self
                    .load_plugins_with_policy(
                        &self.config.plugin_root.to_string_lossy(),
                        PluginLoadPolicy::optional_all()
                    )
                    .map_err(|error|
                        PluginEndpointError::internal(
                            "plugin_discovery_failed",
                            "Plugin discovery failed while resolving the endpoint",
                            error.to_string()
                        )
                    )?;
                resolve_endpoint_route_for(
                    self,
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
        route.runtime.handle_endpoint(request)
    }

    /// Dispatches only to plugins already loaded in this instance. It never starts discovery.
    /// This is the preferred path for isolated tools such as `ezf-plugin-test-host`, where a
    /// misspelled endpoint must not load unrelated libraries from a directory.
    pub fn handle_loaded_endpoint(
        &self,
        request: PluginEndpointRequest
    ) -> Result<PluginEndpointResponse, PluginEndpointError> {
        let endpoint_path = request_endpoint_path(&request.base_path, &request.raw_path);
        let route = resolve_endpoint_route_for(
            self,
            request.method,
            &endpoint_path,
            &request.endpoint_id
        )?.ok_or_else(||
            PluginEndpointError::not_found(
                "plugin_endpoint_not_found",
                "No loaded plugin implements the requested endpoint",
                &format!("No loaded plugin implements {} {}", request.method, endpoint_path)
            )
        )?;
        route.runtime.handle_endpoint(request)
    }
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

fn resolve_endpoint_route_for(
    host: &PluginHostInstance,
    method: PluginContracts::ApiContract::PluginHttpMethod,
    path: &str,
    endpoint_id: &str
) -> Result<Option<PluginEndpointRoute>, PluginEndpointError> {
    let normalized_path = normalize_plugin_path(path).ok();
    let route = normalized_path.and_then(|normalized_path| {
        let manager = host.inner.lock().unwrap_or_else(|p| p.into_inner());
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
