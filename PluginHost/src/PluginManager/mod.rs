//! Plugin registry, endpoint catalog, routing index, and compatibility proxy.
//! Runtime mechanics and concrete DLL loading are isolated in child modules.
mod runtime;
mod executor;

use PluginContracts::{
    ApiContract::{
        normalize_plugin_path,
        PluginCancellation,
        PluginCatalog,
        PluginHttpMethod,
        PluginManifest,
    },
    IPlugin::IPlugin,
};
use super::PluginHostInstance::PluginManagerError;
use glob::{ glob, GlobError };
use std::{ collections::HashMap, path::{ Path, PathBuf }, sync::Arc };

pub use self::runtime::{
    PluginExecutorKind,
    PluginHealthSnapshot,
    PluginHealthState,
    PluginRuntimeConfig,
};
pub(crate) use self::runtime::{ PluginRuntime, PluginUnloadOutcome };

const PLUGIN_AREA_ID: i64 = 0x00ff_ffff;

/// Public trait adapter preserving the existing `IPlugin`-based Factory API while the actual
/// dynamic-library boundary is represented only by `PluginRuntime` and the stable ABI.
#[derive(Clone)]
pub struct PluginProxy {
    runtime: Arc<PluginRuntime>,
}

impl PluginProxy {
    pub(crate) fn new(runtime: Arc<PluginRuntime>) -> Self {
        Self { runtime }
    }
}

impl IPlugin for PluginProxy {
    fn execute_database(
        &self,
        request: DatabaseContracts::DatabaseRequest,
        cancellation: &PluginCancellation
    ) -> Result<DatabaseContracts::DatabaseResponse, DatabaseContracts::DatabaseError> {
        self.runtime.execute_database_with_parent(request, Some(cancellation.as_abi()))
    }

    fn isLoaded(&self) -> bool {
        self.runtime.is_loaded().unwrap_or(false)
    }

    fn manifest(&self) -> PluginManifest {
        self.runtime.manifest().unwrap_or_else(|_| PluginManifest::without_api("UnavailablePlugin"))
    }

    fn handle_endpoint(
        &self,
        request: PluginContracts::ApiContract::PluginEndpointRequest,
        cancellation: &PluginCancellation
    ) -> Result<
        PluginContracts::ApiContract::PluginEndpointResponse,
        PluginContracts::ApiContract::PluginEndpointError
    > {
        self.runtime.handle_endpoint_with_parent(request, Some(cancellation.as_abi()))
    }

    fn name(&self) -> String {
        self.runtime.name().unwrap_or_else(|_| "UnavailablePlugin".to_owned())
    }

    fn to_box(&self) -> Box<dyn IPlugin> {
        Box::new(self.clone())
    }

    fn on_load(&mut self) -> bool {
        self.isLoaded()
    }

    fn on_load_with_host(
        &mut self,
        _host_services: &PluginContracts::ApiContract::PluginHostServices
    ) -> bool {
        self.isLoaded()
    }

    fn on_unload(&mut self, _cancellation: &PluginCancellation) -> bool {
        self.runtime.unload().is_graceful()
    }

    fn on_session_closed(
        &self,
        context: &PluginContracts::ApiContract::PluginSessionContext,
        cancellation: &PluginCancellation
    ) -> Result<(), PluginContracts::ApiContract::PluginLifecycleError> {
        self.runtime.on_session_closed_with_parent(context, Some(cancellation.as_abi()))
    }
}

pub(crate) struct PluginItem {
    pub(crate) path: PathBuf,
    manifest: PluginManifest,
    runtime: Arc<PluginRuntime>,
}

/// Immutable, pre-resolved route target used by the hot request-dispatch path.
/// Keeping an `Arc` here means a request can leave the manager lock with its runtime
/// reference already resolved; unload removes the route index only after a successful
/// runtime shutdown, while requests already holding an `Arc` retain the runtime lifetime.
#[derive(Clone)]
pub(crate) struct PluginEndpointRoute {
    pub(crate) plugin_name: Arc<str>,
    pub(crate) endpoint_id: Arc<str>,
    pub(crate) runtime: Arc<PluginRuntime>,
}

impl PluginItem {
    pub(crate) fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    pub(crate) fn runtime(&self) -> Arc<PluginRuntime> {
        Arc::clone(&self.runtime)
    }

    pub(crate) fn health(&self) -> PluginHealthSnapshot {
        self.runtime.snapshot()
    }
}

#[derive(Clone)]
pub(crate) enum DatabasePluginSelection {
    None,
    Unique {
        runtime: Arc<PluginRuntime>,
    },
    Multiple {
        names: Arc<[String]>,
    },
}

impl Default for DatabasePluginSelection {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Default)]
pub(crate) struct PluginManager {
    pub(crate) paths: HashMap<PathBuf, i64>,
    pub(crate) plugins: HashMap<i64, PluginItem>,
    catalog: PluginCatalog,
    /// `(method, normalized path) -> plugin runtime + endpoint id`.
    /// Rebuilt only when the catalog changes, never as part of an HTTP request.
    endpoint_routes: HashMap<(PluginHttpMethod, String), PluginEndpointRoute>,
    /// Cached DataSource cardinality and unique runtime. Updated atomically with registry changes.
    /// Database requests therefore avoid scanning every loaded plugin under the manager mutex.
    database_plugin: DatabasePluginSelection,
}

impl PluginManager {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn plugin_count(&self) -> usize {
        self.plugins.len()
    }

    pub(crate) fn get_runtime(&self, id: i64) -> Option<Arc<PluginRuntime>> {
        self.plugins.get(&id).map(PluginItem::runtime)
    }

    pub(crate) fn get_manifest(&self, id: i64) -> Option<&PluginManifest> {
        self.plugins.get(&id).map(PluginItem::manifest)
    }

    pub(crate) fn catalog(&self) -> PluginCatalog {
        self.catalog.clone()
    }

    /// Returns only plugin manifests, avoiding the duplicate endpoint-binding list in the
    /// full catalog for API responses that need manifest data only.
    pub(crate) fn plugin_manifests(&self) -> Vec<PluginManifest> {
        self.catalog.plugins.values().cloned().collect()
    }

    /// Performs one hash lookup for an already-normalized path and returns an owned route target.
    /// Path validation/normalization is done before the manager mutex is acquired.
    pub(crate) fn endpoint_route_normalized(
        &self,
        method: PluginHttpMethod,
        normalized_path: String
    ) -> Option<PluginEndpointRoute> {
        self.endpoint_routes.get(&(method, normalized_path)).cloned()
    }

    pub(crate) fn database_plugin_selection(&self) -> DatabasePluginSelection {
        self.database_plugin.clone()
    }

    pub(crate) fn plugin_ids(&self) -> Vec<i64> {
        let mut ids: Vec<i64> = self.plugins.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Discovers plugin library paths without loading or executing any plugin code.
    pub(crate) fn discover_plugin_paths(
        plugin_path: &str
    ) -> Result<Vec<Result<PathBuf, String>>, PluginManagerError> {
        let pattern = Self::build_plugin_pattern(plugin_path);
        let pattern_str = pattern
            .to_str()
            .ok_or_else(|| PluginManagerError::InvalidPluginPath(pattern.clone()))?;

        let entries = glob(pattern_str).map_err(|source| PluginManagerError::InvalidGlobPattern {
            pattern: pattern_str.to_owned(),
            source,
        })?;

        let mut paths = Vec::new();
        let mut errors = Vec::new();

        for entry in entries {
            match entry {
                Ok(path) => paths.push(Self::normalize_path(path.as_path())),
                Err(error) => errors.push(Err(format_glob_error(&error))),
            }
        }

        paths.sort();
        paths.dedup();

        let mut result = paths.into_iter().map(Ok).collect::<Vec<_>>();
        result.extend(errors);
        Ok(result)
    }

    /// Builds a candidate catalog without mutating manager state.
    pub(crate) fn catalog_with_manifest(
        &self,
        manifest: &PluginManifest
    ) -> Result<PluginCatalog, PluginManagerError> {
        let mut manifests = self
            .plugin_ids()
            .into_iter()
            .filter_map(|id| self.get_manifest(id).cloned())
            .collect::<Vec<_>>();
        manifests.push(manifest.clone());
        PluginCatalog::from_manifests(manifests).map_err(PluginManagerError::InvalidCatalog)
    }

    /// Registers a fully initialized plugin. No plugin callback is invoked here.
    pub(crate) fn insert_loaded_plugin(
        &mut self,
        canonical_path: &Path,
        loaded: LoadedPlugin,
        catalog: PluginCatalog
    ) -> i64 {
        let id = self.next_plugin_id();
        let item = PluginItem {
            path: canonical_path.to_path_buf(),
            manifest: loaded.manifest.clone(),
            runtime: loaded.runtime,
        };

        self.plugins.insert(id, item);
        self.paths.insert(canonical_path.to_path_buf(), id);
        self.catalog = catalog;
        self.rebuild_endpoint_routes();
        id
    }

    /// Detaches a plugin from the registry without invoking plugin lifecycle code.
    pub(crate) fn detach_plugin(&mut self, id: i64) -> Result<PluginItem, PluginManagerError> {
        let manifests = self
            .plugin_ids()
            .into_iter()
            .filter(|plugin_id| *plugin_id != id)
            .filter_map(|plugin_id| self.get_manifest(plugin_id).cloned())
            .collect::<Vec<_>>();
        let new_catalog = PluginCatalog::from_manifests(manifests).map_err(
            PluginManagerError::InvalidCatalog
        )?;

        let item = self.plugins.remove(&id).ok_or(PluginManagerError::PluginNotFound { id })?;
        self.paths.remove(&item.path);
        self.catalog = new_catalog;
        self.rebuild_endpoint_routes();
        Ok(item)
    }

    /// Rebuild the hot-path route index after a validated catalog mutation. This is deliberately
    /// O(number of registered endpoints) on load/unload, which are rare operations, instead of
    /// cloning the full catalog and linearly normalizing every endpoint on every request.
    fn rebuild_endpoint_routes(&mut self) {
        let runtimes_by_name = self.plugins
            .values()
            .map(|item| { (item.manifest.name.to_ascii_lowercase(), item.runtime()) })
            .collect::<HashMap<_, _>>();

        let mut routes = HashMap::with_capacity(self.catalog.endpoints.len());
        for binding in &self.catalog.endpoints {
            let Some(runtime) = runtimes_by_name.get(
                &binding.plugin_name.to_ascii_lowercase()
            ) else {
                // Catalog and registered plugins are updated under the same manager lock. A
                // missing runtime would indicate an internal invariant violation, not a request
                // condition; skip the route rather than panic during plugin lifecycle changes.
                continue;
            };
            let Ok(normalized_path) = normalize_plugin_path(&binding.endpoint.path) else {
                // Catalog construction validates paths before a mutation is committed.
                continue;
            };

            routes.insert((binding.endpoint.method, normalized_path), PluginEndpointRoute {
                // Route hits clone only Arcs; no String allocation is required per request.
                plugin_name: Arc::<str>::from(binding.plugin_name.as_str()),
                endpoint_id: Arc::<str>::from(binding.endpoint.id.as_str()),
                runtime: Arc::clone(runtime),
            });
        }

        debug_assert_eq!(
            routes.len(),
            self.catalog.endpoints.len(),
            "a validated catalog must have a runtime route for every endpoint"
        );
        self.endpoint_routes = routes;

        // This selection is rebuilt under the same manager lock as the plugin registry and
        // endpoint index, so a request sees one consistent registry generation.
        let data_sources = self
            .plugin_ids()
            .into_iter()
            .filter_map(|id| {
                let manifest = self.get_manifest(id)?;
                if manifest.kind != PluginContracts::ApiContract::PluginKind::DataSource {
                    return None;
                }
                self.get_runtime(id).map(|runtime| (manifest.name.clone(), runtime))
            })
            .collect::<Vec<_>>();
        self.database_plugin = match data_sources.len() {
            0 => DatabasePluginSelection::None,
            1 =>
                DatabasePluginSelection::Unique {
                    runtime: Arc::clone(&data_sources[0].1),
                },
            _ =>
                DatabasePluginSelection::Multiple {
                    names: Arc::<[String]>::from(
                        data_sources
                            .into_iter()
                            .map(|(name, _)| name)
                            .collect::<Vec<_>>()
                    ),
                },
        };
    }

    pub(crate) fn load_plugin(
        path: &Path,
        config: PluginRuntimeConfig,
        executor_selection: crate::PluginHostInstance::PluginExecutorSelection,
        process_executor: crate::PluginHostInstance::ProcessExecutorConfig,
        database: Option<PluginContracts::ApiContract::PluginDatabaseCapability>,
        database_dispatcher: crate::PluginHostInstance::DatabaseDispatcher
    ) -> Result<LoadedPlugin, PluginManagerError> {
        executor::load_plugin(
            path,
            config,
            executor_selection,
            process_executor,
            database,
            database_dispatcher
        )
    }

    fn next_plugin_id(&self) -> i64 {
        self.plugins
            .keys()
            .copied()
            .max()
            .map_or(PLUGIN_AREA_ID, |max_id| max_id + 1)
    }

    fn build_plugin_pattern(plugin_path: &str) -> PathBuf {
        Path::new(plugin_path).join(Self::library_glob())
    }

    fn library_glob() -> &'static str {
        #[cfg(target_os = "linux")]
        {
            "*.so"
        }
        #[cfg(target_os = "macos")]
        {
            "*.dylib"
        }
        #[cfg(target_os = "windows")]
        {
            "*.dll"
        }
    }

    pub(crate) fn normalize_path(path: &Path) -> PathBuf {
        path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
    }
}

pub(crate) struct LoadedPlugin {
    pub(super) runtime: Arc<PluginRuntime>,
    pub(super) manifest: PluginManifest,
}

impl LoadedPlugin {
    pub(crate) fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }
}

fn format_glob_error(error: &GlobError) -> String {
    match error.path().to_str() {
        Some(path) => format!("{} ({})", path, error.error()),
        None => error.error().to_string(),
    }
}
