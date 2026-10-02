#![allow(non_snake_case)]

use DatabaseContracts::{ DatabaseError, DatabaseRequest, DatabaseResponse };
use std::sync::atomic::{ AtomicBool, AtomicUsize, Ordering };
use std::{ fs::OpenOptions, io::Write, path::PathBuf, thread, time::Duration };

use PluginContracts::{
    declare_plugin,
    ApiContract::{
        PluginEndpointError,
        PluginEndpointRequest,
        PluginEndpointResponse,
        PluginHttpMethod,
        PluginKind,
        PluginManifest,
        PluginCancellation,
        PluginEndpointDescriptor,
        PluginLifecycleError,
        PluginSessionContext,
    },
    IPlugin::IPlugin,
};

static ACTIVE_CALLS: AtomicUsize = AtomicUsize::new(0);
static MAX_ACTIVE_CALLS: AtomicUsize = AtomicUsize::new(0);
static FAIL_UNLOAD_ONCE: AtomicBool = AtomicBool::new(true);
static FAIL_DESTROY_ONCE: AtomicBool = AtomicBool::new(true);
static SLOW_ON_UNLOAD_ONCE: AtomicBool = AtomicBool::new(true);
static SLOW_DESTROY_ONCE: AtomicBool = AtomicBool::new(true);
static NEXT_WORKER_THREAD_ID: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static WORKER_THREAD_ID: usize = NEXT_WORKER_THREAD_ID.fetch_add(1, Ordering::AcqRel);
}

fn current_worker_thread_id() -> usize {
    WORKER_THREAD_ID.with(|id| *id)
}

#[derive(Clone, Default)]
struct PluginHostIntegration {
    loaded: bool,
    plugin_directory: Option<PathBuf>,
    database: Option<PluginContracts::ApiContract::PluginDatabaseCapability>,
    mode: String,
}

fn record_peak_active(active: usize) {
    let mut peak = MAX_ACTIVE_CALLS.load(Ordering::Acquire);
    while active > peak {
        match MAX_ACTIVE_CALLS.compare_exchange(peak, active, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => {
                break;
            }
            Err(current) => {
                peak = current;
            }
        }
    }
}

impl IPlugin for PluginHostIntegration {
    fn name(&self) -> String {
        if self.mode == "slow_name" {
            for _ in 0..30 {
                if PluginContracts::ApiContract::current_plugin_cancellation()
                    .is_some_and(|cancellation| cancellation.is_cancelled())
                {
                    return "PluginHostIntegration".to_owned();
                }
                thread::sleep(Duration::from_millis(5));
            }
        }
        match self.mode.as_str() {
            "catalog_conflict" => "PluginHostIntegrationConflict".to_owned(),
            "database" => "PluginHostDatabaseBridge".to_owned(),
            _ => "PluginHostIntegration".to_owned(),
        }
    }

    fn to_box(&self) -> Box<dyn IPlugin> {
        Box::new(self.clone())
    }

    fn on_load(&mut self) -> bool {
        self.record_lifecycle_event("on_load");
        self.loaded = true;
        self.loaded
    }

    fn on_load_with_host(
        &mut self,
        host_services: &PluginContracts::ApiContract::PluginHostServices
    ) -> bool {
        self.plugin_directory = host_services.plugin_directory();
        self.database = host_services.database;
        self.mode = std::env::var("EZF_PLUGIN_FIXTURE_MODE").unwrap_or_default();
        if self.mode == "catalog_conflict" {
            self.write_marker("load-conflict.marker");
        } else {
            self.write_marker("load.marker");
        }
        self.on_load()
    }

    fn on_unload(&mut self, cancellation: &PluginCancellation) -> bool {
        self.record_lifecycle_event("on_unload");
        self.write_marker("unload.marker");
        if self.mode == "slow_on_unload" && SLOW_ON_UNLOAD_ONCE.swap(false, Ordering::AcqRel) {
            for _ in 0..30 {
                if cancellation.is_cancelled() {
                    self.write_marker("unload-cancelled.marker");
                    break;
                }
                thread::sleep(Duration::from_millis(5));
            }
        }
        if self.mode == "on_unload_failure" && FAIL_UNLOAD_ONCE.swap(false, Ordering::AcqRel) {
            return false;
        }
        self.loaded = false;
        true
    }

    fn on_destroy(&mut self, cancellation: &PluginCancellation) -> bool {
        self.record_lifecycle_event("on_destroy");
        self.write_marker("destroy.marker");
        if self.mode == "slow_destroy" && SLOW_DESTROY_ONCE.swap(false, Ordering::AcqRel) {
            for _ in 0..30 {
                if cancellation.is_cancelled() {
                    self.write_marker("destroy-cancelled.marker");
                    break;
                }
                thread::sleep(Duration::from_millis(5));
            }
        }
        if self.mode == "destroy_failure" && FAIL_DESTROY_ONCE.swap(false, Ordering::AcqRel) {
            return false;
        }
        true
    }

    fn isLoaded(&self) -> bool {
        self.loaded
    }

    fn on_session_closed(
        &self,
        context: &PluginSessionContext,
        cancellation: &PluginCancellation
    ) -> Result<(), PluginLifecycleError> {
        if cancellation.is_cancelled() {
            self.write_marker("session-cancelled.marker");
            return Err(PluginLifecycleError::new("plugin_cancelled", "session cleanup cancelled"));
        }
        if self.mode == "session_failure" {
            return Err(
                PluginLifecycleError::new(
                    "fixture_session_failure",
                    "session cleanup intentionally failed"
                )
            );
        }

        self.write_marker(&format!("session-closed-{}.marker", context.session_id));
        Ok(())
    }

    fn manifest(&self) -> PluginManifest {
        if self.mode == "slow_manifest" {
            for _ in 0..30 {
                if PluginContracts::ApiContract::current_plugin_cancellation()
                    .is_some_and(|cancellation| cancellation.is_cancelled())
                {
                    return PluginManifest::without_api("PluginHostIntegration");
                }
                thread::sleep(Duration::from_millis(5));
            }
        }
        let (name, kind) = if self.mode == "catalog_conflict" {
            ("PluginHostIntegrationConflict".to_owned(), PluginKind::Domain)
        } else if self.mode == "database" {
            ("PluginHostDatabaseBridge".to_owned(), PluginKind::DataSource)
        } else {
            (self.name(), PluginKind::Domain)
        };
        let mut manifest = PluginManifest::new(name, "0.0.1", kind);
        manifest.endpoints.push(
            PluginEndpointDescriptor::get("ping", "/pluginhost-integration/ping").documented(
                "PluginHostIntegration",
                "Ping",
                Some("Minimal endpoint used by PluginHost lifecycle integration tests")
            )
        );
        manifest.endpoints.push(
            PluginEndpointDescriptor::get("panic", "/pluginhost-integration/panic").documented(
                "PluginHostIntegration",
                "Panic",
                Some("Endpoint used to verify ABI panic isolation")
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
        request: PluginEndpointRequest,
        cancellation: &PluginCancellation
    ) -> Result<PluginEndpointResponse, PluginEndpointError> {
        if request.method != PluginHttpMethod::Get {
            return Err(PluginEndpointError::not_supported(&request.endpoint_id));
        }

        match request.endpoint_id.as_str() {
            "ping" if request.raw_path == "/pluginhost-integration/ping" =>
                PluginEndpointResponse::json(
                    200,
                    &serde_json::json!({
                        "plugin": self.name(),
                        "loaded": self.loaded,
                        "endpoint": "ping",
                        "worker_thread": thread::current().name().unwrap_or("unnamed"),
                        "worker_thread_id": current_worker_thread_id()
                    })
                ),
            "panic" if self.mode == "panic" => {
                panic!("intentional PluginHost integration panic");
            }
            "slow" if request.raw_path == "/pluginhost-integration/slow" => {
                let active = ACTIVE_CALLS.fetch_add(1, Ordering::AcqRel) + 1;
                record_peak_active(active);
                if let Some(directory) = &self.plugin_directory {
                    let _ = std::fs::write(directory.join("slow-entered.marker"), b"entered");
                }
                let ignore_cancellation = self.mode == "slow_ignore_cancel";
                for _ in 0..60 {
                    if !ignore_cancellation && cancellation.is_cancelled() {
                        if let Some(directory) = &self.plugin_directory {
                            let _ = std::fs::write(directory.join("slow-cancelled.marker"), b"cancelled");
                        }
                        ACTIVE_CALLS.fetch_sub(1, Ordering::AcqRel);
                        return Err(PluginEndpointError::new(499, "plugin_cancelled", "plugin call cancelled", None));
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                if let Some(directory) = &self.plugin_directory {
                    let _ = std::fs::write(directory.join("slow-exited.marker"), b"exited");
                }
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
        request: DatabaseRequest,
        cancellation: &PluginCancellation
    ) -> Result<DatabaseResponse, DatabaseError> {
        if self.mode != "database" {
            return Err(
                DatabaseError::unsupported(
                    "PluginHostIntegration test fixture does not implement database execution"
                )
            );
        }

        let Some(database) = self.database else {
            return Err(
                DatabaseError::unsupported(
                    "database host capability was not supplied to the plugin"
                )
            );
        };

        let request_bytes = serde_json
            ::to_vec(&request)
            .map_err(|error|
                DatabaseError::new(
                    DatabaseContracts::DatabaseErrorKind::InvalidRequest,
                    error.to_string()
                )
            )?;
        let mut response = PluginContracts::PluginAbi::AbiBuffer::EMPTY;
        let status = unsafe {
            (database.execute)(
                PluginContracts::PluginAbi::AbiSlice::from_bytes(&request_bytes),
                cancellation.as_abi(),
                &mut response
            )
        };
        if status != PluginContracts::PluginAbi::PLUGIN_STATUS_OK {
            if !response.ptr.is_null() {
                unsafe {
                    (database.free)(response);
                }
            }
            return Err(
                DatabaseError::new(
                    DatabaseContracts::DatabaseErrorKind::Query,
                    format!("host database capability returned ABI status {status}")
                )
            );
        }

        let bytes = (unsafe { response.to_vec() }).ok_or_else(||
            DatabaseError::new(
                DatabaseContracts::DatabaseErrorKind::Decode,
                "invalid host database response buffer"
            )
        )?;
        unsafe {
            (database.free)(response);
        }
        serde_json
            ::from_slice::<Result<DatabaseResponse, DatabaseError>>(&bytes)
            .map_err(|error|
                DatabaseError::new(DatabaseContracts::DatabaseErrorKind::Decode, error.to_string())
            )?
    }
}

impl PluginHostIntegration {
    fn new() -> Self {
        Self {
            loaded: false,
            plugin_directory: None,
            database: None,
            mode: String::new(),
        }
    }

    fn write_marker(&self, file_name: &str) {
        if let Some(directory) = &self.plugin_directory {
            let _ = std::fs::write(directory.join(file_name), b"marker");
        }
    }

    fn record_lifecycle_event(&self, event: &str) {
        let Some(directory) = &self.plugin_directory else {
            return;
        };
        let path = directory.join("lifecycle.order");
        let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
            return;
        };
        let _ = writeln!(file, "{event}");
    }
}

fn create_plugin() -> Box<dyn IPlugin> {
    Box::new(PluginHostIntegration::new())
}

declare_plugin!(create_plugin);
