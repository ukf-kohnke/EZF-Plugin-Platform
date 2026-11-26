//! Public load, health-report, and error types for the PluginHostInstance runtime.
use super::*;
use PluginContracts::ApiContract::{PluginHttpMethod, PluginKind};

/// Immutable view of the API endpoints currently exposed by successfully loaded Domain plugins.
///
/// The snapshot is rebuilt together with the PluginHost route index whenever the plugin
/// registry changes. It deliberately excludes DataSource plugins and Domain plugins without
/// API endpoints.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActiveDomainApiSnapshot {
    endpoints: Vec<ActiveDomainApiEndpoint>,
    plugin_names: Vec<String>,
    missing_registrations: Vec<ActiveDomainApiEndpoint>,
}

/// One endpoint declared by an active Domain plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveDomainApiEndpoint {
    pub plugin_name: String,
    pub endpoint_id: String,
    pub method: PluginHttpMethod,
    pub path: String,
}

impl ActiveDomainApiEndpoint {
    /// Stable human-readable representation used in readiness diagnostics.
    pub fn display_route(&self) -> String {
        format!("{} {}", self.method, self.path)
    }
}

impl ActiveDomainApiSnapshot {
    pub fn endpoints(&self) -> &[ActiveDomainApiEndpoint] {
        &self.endpoints
    }

    pub fn plugin_names(&self) -> &[String] {
        &self.plugin_names
    }

    /// Manifest endpoints for which the runtime route index has no matching entry.
    /// This should be empty for every successfully validated registry generation.
    pub fn missing_registrations(&self) -> &[ActiveDomainApiEndpoint] {
        &self.missing_registrations
    }

    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }

    pub(crate) fn from_catalog(
        catalog: &PluginCatalog,
        registered_routes: &std::collections::HashSet<(PluginHttpMethod, String)>
    ) -> Self {
        let mut snapshot = Self::default();
        let mut plugin_names = std::collections::BTreeSet::new();

        for binding in &catalog.endpoints {
            let Some(manifest) = catalog.plugins.get(&binding.plugin_name) else {
                continue;
            };
            if manifest.kind != PluginKind::Domain {
                continue;
            }

            let endpoint = ActiveDomainApiEndpoint {
                plugin_name: binding.plugin_name.clone(),
                endpoint_id: binding.endpoint.id.clone(),
                method: binding.endpoint.method,
                path: binding.endpoint.path.clone(),
            };
            plugin_names.insert(binding.plugin_name.clone());

            let route_is_registered = normalize_plugin_path(&binding.endpoint.path)
                .ok()
                .is_some_and(|path| registered_routes.contains(&(binding.endpoint.method, path)));
            if !route_is_registered {
                snapshot.missing_registrations.push(endpoint.clone());
            }
            snapshot.endpoints.push(endpoint);
        }

        snapshot.plugin_names = plugin_names.into_iter().collect();
        snapshot
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginSessionNotificationFailure {
    pub plugin_name: String,
    pub error: PluginLifecycleError,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PluginSessionNotificationReport {
    pub notified_plugins: Vec<String>,
    pub failures: Vec<PluginSessionNotificationFailure>,
}

impl PluginSessionNotificationReport {
    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginRequirement {
    Required,
    Optional,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginLoadFailure {
    pub path: PathBuf,
    pub plugin_name: Option<String>,
    pub requirement: PluginRequirement,
    pub error: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PluginLoadReport {
    pub requested_path: PathBuf,
    pub discovered: Vec<PathBuf>,
    pub loaded: Vec<String>,
    pub skipped_already_loaded: Vec<PathBuf>,
    pub failures: Vec<PluginLoadFailure>,
}

impl PluginLoadReport {
    pub fn required_failures(&self) -> Vec<&PluginLoadFailure> {
        self.failures
            .iter()
            .filter(|failure| failure.requirement == PluginRequirement::Required)
            .collect()
    }

    pub fn has_required_failures(&self) -> bool {
        self.failures.iter().any(|failure| failure.requirement == PluginRequirement::Required)
    }

    pub fn is_success(&self) -> bool {
        !self.has_required_failures()
    }

    pub fn loaded_count(&self) -> usize {
        self.loaded.len()
    }

    pub fn failure_count(&self) -> usize {
        self.failures.len()
    }

    pub(super) fn summary(&self) -> String {
        format!(
            "plugin load report for '{}': discovered={}, loaded={}, skipped={}, failures={}, required_failures={}",
            self.requested_path.display(),
            self.discovered.len(),
            self.loaded.len(),
            self.skipped_already_loaded.len(),
            self.failures.len(),
            self.required_failures().len()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginLoadPolicy {
    pub(super) default_requirement: PluginRequirement,
    pub(super) overrides: BTreeMap<String, PluginRequirement>,
}

impl PluginLoadPolicy {
    /// Treat every discovered plugin as required unless explicitly overridden.
    pub fn required_all() -> Self {
        Self {
            default_requirement: PluginRequirement::Required,
            overrides: BTreeMap::new(),
        }
    }

    /// Treat every discovered plugin as optional unless explicitly required.
    pub fn optional_all() -> Self {
        Self {
            default_requirement: PluginRequirement::Optional,
            overrides: BTreeMap::new(),
        }
    }

    pub fn require(mut self, plugin_name: impl Into<String>) -> Self {
        self.overrides.insert(
            normalize_plugin_name(&plugin_name.into()),
            PluginRequirement::Required
        );
        self
    }

    pub fn optional(mut self, plugin_name: impl Into<String>) -> Self {
        self.overrides.insert(
            normalize_plugin_name(&plugin_name.into()),
            PluginRequirement::Optional
        );
        self
    }

    pub fn requirement_for_name(&self, plugin_name: &str) -> PluginRequirement {
        self.overrides
            .get(&normalize_plugin_name(plugin_name))
            .copied()
            .unwrap_or(self.default_requirement)
    }

    pub fn requirement_for_path(&self, path: &Path) -> PluginRequirement {
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .map(|stem| self.requirement_for_name(stem))
            .unwrap_or(self.default_requirement)
    }
}

pub(super) fn normalize_plugin_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

#[derive(Debug)]
pub enum PluginManagerError {
    InvalidCatalog(PluginCatalogError),
    InvalidPluginPath(PathBuf),
    InvalidGlobPattern {
        pattern: String,
        source: PatternError,
    },
    LibraryLoad {
        path: PathBuf,
        source: libloading::Error,
    },
    ConstructorLoad {
        path: PathBuf,
        symbol: &'static str,
        source: libloading::Error,
    },
    PluginAbiIncompatible {
        path: PathBuf,
        message: String,
    },
    PluginAbiCall {
        path: PathBuf,
        message: String,
    },
    PluginNameMismatch {
        path: PathBuf,
        runtime_name: String,
        manifest_name: String,
    },
    PluginInitializationFailed {
        path: PathBuf,
    },
    PluginLoadPolicyViolation {
        report: PluginLoadReport,
    },
    PluginLoadInProgress {
        path: PathBuf,
    },
    PluginAlreadyLoaded {
        path: PathBuf,
        id: i64,
    },
    PluginNotFound {
        id: i64,
    },
    PluginUnloadFailed {
        id: i64,
    },
}

impl std::fmt::Display for PluginManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginManagerError::InvalidCatalog(error) =>
                write!(f, "Invalid catalog configuration: {:?}", error),
            PluginManagerError::InvalidPluginPath(path) =>
                write!(f, "Invalid plugin path: {:?}", path),
            PluginManagerError::InvalidGlobPattern { pattern, source } =>
                write!(f, "Invalid glob pattern '{}': {}", pattern, source),
            PluginManagerError::LibraryLoad { path, source } =>
                write!(f, "Failed to load library at {:?}: {}", path, source),
            PluginManagerError::ConstructorLoad { path, symbol, source } =>
                write!(f, "Failed to load constructor '{}' at {:?}: {}", symbol, path, source),
            PluginManagerError::PluginAbiIncompatible { path, message } =>
                write!(f, "Plugin at {:?} exposes an incompatible ABI: {}", path, message),
            PluginManagerError::PluginAbiCall { path, message } =>
                write!(f, "Plugin ABI call failed at {:?}: {}", path, message),
            PluginManagerError::PluginNameMismatch { path, runtime_name, manifest_name } =>
                write!(
                    f,
                    "Failed to match plugin '{:?}' , {:?} : {}",
                    path,
                    runtime_name,
                    manifest_name
                ),
            PluginManagerError::PluginInitializationFailed { path } =>
                write!(f, "Plugin initialization failed at {:?}", path),
            PluginManagerError::PluginLoadPolicyViolation { report } =>
                write!(f, "{}", report.summary()),
            PluginManagerError::PluginLoadInProgress { path } =>
                write!(f, "Plugin at {:?} is already being loaded", path),
            PluginManagerError::PluginAlreadyLoaded { path, id } =>
                write!(f, "Plugin already loaded at {:?} with id {}", path, id),
            PluginManagerError::PluginNotFound { id } =>
                write!(f, "Plugin not found with id {}", id),
            PluginManagerError::PluginUnloadFailed { id } =>
                write!(f, "Failed to unload plugin with id {}", id),
        }
    }
}

impl std::error::Error for PluginManagerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PluginManagerError::InvalidGlobPattern { source, .. } => Some(source),
            PluginManagerError::LibraryLoad { source, .. } => Some(source),
            PluginManagerError::ConstructorLoad { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod active_domain_api_snapshot_tests {
    use super::*;
    use PluginContracts::ApiContract::{PluginCatalog, PluginEndpointDescriptor, PluginManifest};
    use std::collections::HashSet;

    #[test]
    fn snapshot_contains_only_active_domain_api_endpoints_and_reports_route_gaps() {
        let mut domain = PluginManifest::new("PatientDomain", "1.0.0", PluginKind::Domain);
        domain.endpoints.push(PluginEndpointDescriptor::get("list", "/patients"));
        domain.endpoints.push(PluginEndpointDescriptor::post("persist", "/patients"));

        let mut data_source = PluginManifest::new("Database", "1.0.0", PluginKind::DataSource);
        data_source.endpoints.push(PluginEndpointDescriptor::get("query", "/database/query"));

        let domain_library = PluginManifest::new("DomainLibrary", "1.0.0", PluginKind::Domain);
        let catalog = PluginCatalog::from_manifests([domain, data_source, domain_library])
            .expect("fixture catalog should be valid");

        let registered_routes = HashSet::from([
            (PluginHttpMethod::Get, "/patients".to_owned()),
        ]);
        let snapshot = ActiveDomainApiSnapshot::from_catalog(&catalog, &registered_routes);

        assert_eq!(snapshot.endpoints().len(), 2);
        assert_eq!(snapshot.plugin_names(), &["PatientDomain".to_owned()]);
        assert_eq!(snapshot.missing_registrations().len(), 1);
        assert_eq!(snapshot.missing_registrations()[0].display_route(), "POST /patients");
        assert!(snapshot.endpoints().iter().all(|endpoint|
            endpoint.plugin_name == "PatientDomain"
        ));
    }

    #[test]
    fn snapshot_is_empty_when_no_domain_plugin_exposes_api_endpoints() {
        let domain_library = PluginManifest::new("DomainLibrary", "1.0.0", PluginKind::Domain);
        let data_source = PluginManifest::new("Database", "1.0.0", PluginKind::DataSource);
        let catalog = PluginCatalog::from_manifests([domain_library, data_source])
            .expect("fixture catalog should be valid");
        let snapshot = ActiveDomainApiSnapshot::from_catalog(&catalog, &HashSet::new());

        assert!(snapshot.is_empty());
        assert!(snapshot.plugin_names().is_empty());
        assert!(snapshot.missing_registrations().is_empty());
    }
}
