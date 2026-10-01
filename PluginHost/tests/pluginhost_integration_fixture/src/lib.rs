#![allow(non_snake_case)]

use DatabaseContracts::{ DatabaseError, DatabaseRequest, DatabaseResponse };
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{path::PathBuf, thread, time::Duration};

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

static ACTIVE_CALLS: AtomicUsize = AtomicUsize::new(0);
static MAX_ACTIVE_CALLS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Default)]
struct PluginHostIntegration {
    loaded: bool,
    plugin_directory: Option<PathBuf>,
}

fn record_peak_active(active: usize) {
    let mut peak = MAX_ACTIVE_CALLS.load(Ordering::Acquire);
    while active > peak {
        match MAX_ACTIVE_CALLS.compare_exchange(peak, active, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => break,
            Err(current) => peak = current,
        }
    }
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

    fn on_load_with_host(&mut self, host_services: &PluginContracts::ApiContract::PluginHostServices) -> bool {
        self.plugin_directory = host_services.plugin_directory();
        self.on_load()
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
        manifest.endpoints.push(
            PluginEndpointDescriptor::get("slow", "/pluginhost-integration/slow").documented(
                "PluginHostIntegration",
                "Slow",
                Some("Endpoint used to verify concurrent calls and unload quiescing")
            )
        );
        manifest
    }

    fn handle_endpoint(
        &self,
        request: PluginEndpointRequest
    ) -> Result<PluginEndpointResponse, PluginEndpointError> {
        if
            request.method != PluginHttpMethod::Get
        {
            return Err(PluginEndpointError::not_supported(&request.endpoint_id));
        }

        match request.endpoint_id.as_str() {
            "ping" if request.raw_path == "/pluginhost-integration/ping" =>
                PluginEndpointResponse::json(
                    200,
                    &serde_json::json!({
                        "plugin": self.name(),
                        "loaded": self.loaded,
                        "endpoint": "ping"
                    })
                ),
            "slow" if request.raw_path == "/pluginhost-integration/slow" => {
                let active = ACTIVE_CALLS.fetch_add(1, Ordering::AcqRel) + 1;
                record_peak_active(active);
                if let Some(directory) = &self.plugin_directory {
                    let _ = std::fs::write(directory.join("slow-entered.marker"), b"entered");
                }
                thread::sleep(Duration::from_millis(300));
                let peak = MAX_ACTIVE_CALLS.load(Ordering::Acquire);
                ACTIVE_CALLS.fetch_sub(1, Ordering::AcqRel);
                PluginEndpointResponse::json(
                    200,
                    &serde_json::json!({
                        "plugin": self.name(),
                        "loaded": self.loaded,
                        "endpoint": "slow",
                        "peak_active": peak
                    })
                )
            }
            _ => Err(PluginEndpointError::not_supported(&request.endpoint_id)),
        }
    }

    fn execute_database(
        &self,
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
