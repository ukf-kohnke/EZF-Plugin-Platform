//! Public load, health-report, and error types for the PluginHost factory.
use super::*;

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
