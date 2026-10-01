#![allow(non_snake_case)]

use DatabaseContracts::{ DatabaseError, DatabaseRequest, DatabaseResponse };
use PluginContracts::{
    declare_plugin,
    ApiContract::{
        PluginEndpointError,
        PluginEndpointRequest,
        PluginEndpointResponse,
        PluginHttpMethod,
        PluginKind,
        PluginManifest,
        PluginEndpointDescriptor,
    },
    IPlugin::IPlugin,
};

#[derive(Clone, Default)]
struct PluginHostIntegration {
    loaded: bool,
}

impl IPlugin for PluginHostIntegration {
    fn name(&self) -> String {
        "PluginHostIntegration".to_owned()
    }

    fn to_box(&self) -> Box<dyn IPlugin> {
        Box::new(self.clone())
    }

    fn on_load(&mut self) -> bool {
        self.loaded = true;
        self.loaded
    }

    fn on_unload(&mut self) -> bool {
        self.loaded = false;
        !self.loaded
    }

    fn isLoaded(&self) -> bool {
        self.loaded
    }

    fn manifest(&self) -> PluginManifest {
        let mut manifest = PluginManifest::new(self.name(), "0.0.1", PluginKind::Domain);
        manifest.endpoints.push(
            PluginEndpointDescriptor::get("ping", "/pluginhost-integration/ping").documented(
                "PluginHostIntegration",
                "Ping",
                Some("Minimal endpoint used by PluginHost lifecycle integration tests")
            )
        );
        manifest
    }

    fn handle_endpoint(
        &mut self,
        request: PluginEndpointRequest
    ) -> Result<PluginEndpointResponse, PluginEndpointError> {
        if
            request.endpoint_id != "ping" ||
            request.method != PluginHttpMethod::Get ||
            request.raw_path != "/pluginhost-integration/ping"
        {
            return Err(PluginEndpointError::not_supported(&request.endpoint_id));
        }

        PluginEndpointResponse::json(
            200,
            &serde_json::json!({
                "plugin": self.name(),
                "loaded": self.loaded,
                "endpoint": "ping"
            })
        )
    }

    fn execute_database(
        &mut self,
        _request: DatabaseRequest
    ) -> Result<DatabaseResponse, DatabaseError> {
        Err(
            DatabaseError::unsupported(
                "PluginHostIntegration test fixture does not implement database execution"
            )
        )
    }
}

impl PluginHostIntegration {
    fn new() -> Self {
        Self::default()
    }
}

fn create_plugin() -> Box<dyn IPlugin> {
    Box::new(PluginHostIntegration::new())
}

declare_plugin!(create_plugin);
