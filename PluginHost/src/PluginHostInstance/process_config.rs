//! Explicit process-executor and worker launch configuration.
//!
//! This module intentionally does not read environment variables. The application composition
//! root (or a CLI adapter) translates deployment settings into these typed values before creating
//! a PluginHostInstance.

use PluginIpc::SandboxProfile;
use std::{ collections::BTreeMap, ffi::OsString, path::PathBuf, time::Duration };

const DEFAULT_CANCEL_GRACE: Duration = Duration::from_millis(500);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const MIN_CANCEL_GRACE: Duration = Duration::from_millis(10);
const MAX_CANCEL_GRACE: Duration = Duration::from_secs(30);
const MIN_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);

/// Sandbox policy and filesystem/network permissions for process-backed plugins.
#[derive(Debug, Clone, Default)]
pub struct SandboxConfig {
    /// Default profile used when no plugin-specific profile is configured.
    pub default_profile: Option<SandboxProfile>,
    /// Optional profile overrides keyed by canonical absolute plugin-library path.
    /// If this map is non-empty, every loaded plugin must have an entry.
    pub profiles_by_plugin: BTreeMap<PathBuf, SandboxProfile>,
    /// Requires a per-plugin profile mapping even when the map is empty. This preserves the
    /// distinction between an absent legacy mapping and an explicitly configured empty mapping.
    pub require_per_plugin_profile: bool,
    /// Additional filesystem paths readable by the worker.
    pub extra_read_paths: Vec<PathBuf>,
    /// Trusted dynamic-library search paths. These are also added to the read allowlist.
    pub library_paths: Vec<PathBuf>,
    /// Outbound TCP destination ports allowed for DataSource workers.
    pub datasource_tcp_ports: Vec<u16>,
    /// True only when a deployment firewall/container enforces destination-IP egress policy.
    pub datasource_egress_policy_acknowledged: bool,
    /// Explicit opt-in for development-only execution without an OS sandbox.
    pub allow_unconfined: bool,
}

/// Explicit values passed to a worker process.
///
/// Enforced sandbox modes clear the inherited environment and pass only these variables.
/// DevelopmentUnconfined intentionally retains the parent environment for local debugging and
/// overlays these explicitly configured values.
#[derive(Debug, Clone, Default)]
pub struct WorkerEnvironment {
    pub variables: BTreeMap<String, OsString>,
}

/// All inputs that control one ProcessExecutor launch.
///
/// `worker_binary == None` means resolve `ezf-plugin-worker` beside the current host executable
/// (with the Cargo `target/<profile>` fallback for test executables). The executor does not read
/// environment variables when consuming this configuration.
#[derive(Debug, Clone)]
pub struct ProcessExecutorConfig {
    pub worker_binary: Option<PathBuf>,
    pub sandbox: SandboxConfig,
    pub cancellation_grace: Duration,
    pub shutdown_timeout: Duration,
    pub worker_environment: WorkerEnvironment,
}

impl Default for ProcessExecutorConfig {
    fn default() -> Self {
        Self {
            worker_binary: None,
            sandbox: SandboxConfig::default(),
            cancellation_grace: DEFAULT_CANCEL_GRACE,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            worker_environment: WorkerEnvironment::default(),
        }
    }
}

impl ProcessExecutorConfig {
    /// Validates time budgets and the explicitly configured worker path.
    /// Filesystem and sandbox policy validation happens immediately before worker creation,
    /// after the concrete plugin-library path is known.
    pub fn validate(&self) -> Result<(), String> {
        if self.cancellation_grace < MIN_CANCEL_GRACE || self.cancellation_grace > MAX_CANCEL_GRACE {
            return Err(
                format!(
                    "process cancellation grace must be between {:?} and {:?}",
                    MIN_CANCEL_GRACE,
                    MAX_CANCEL_GRACE
                )
            );
        }
        if
            self.shutdown_timeout < MIN_SHUTDOWN_TIMEOUT ||
            self.shutdown_timeout > MAX_SHUTDOWN_TIMEOUT
        {
            return Err(
                format!(
                    "process shutdown timeout must be between {:?} and {:?}",
                    MIN_SHUTDOWN_TIMEOUT,
                    MAX_SHUTDOWN_TIMEOUT
                )
            );
        }
        if let Some(worker) = self.worker_binary.as_ref() {
            if !worker.is_absolute() {
                return Err(
                    "ProcessExecutorConfig.worker_binary must be an absolute path".to_owned()
                );
            }
            if !worker.is_file() {
                return Err(
                    format!(
                        "ProcessExecutorConfig.worker_binary does not name a file: {}",
                        worker.display()
                    )
                );
            }
        }
        if self.sandbox.datasource_tcp_ports.iter().any(|port| *port == 0) {
            return Err("DataSource TCP ports must be between 1 and 65535".to_owned());
        }
        if self.sandbox.datasource_tcp_ports.len() > 32 {
            return Err("DataSource TCP port allowlist may contain at most 32 ports".to_owned());
        }
        if self.sandbox.profiles_by_plugin.keys().any(|path| !path.is_absolute()) {
            return Err(
                "SandboxConfig.profiles_by_plugin keys must be absolute canonical plugin paths".to_owned()
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ProcessExecutorConfig;
    use std::time::Duration;

    #[test]
    fn default_process_configuration_is_valid() {
        assert!(ProcessExecutorConfig::default().validate().is_ok());
    }

    #[test]
    fn cancellation_grace_is_bounded() {
        let mut config = ProcessExecutorConfig::default();
        config.cancellation_grace = Duration::ZERO;
        assert!(config.validate().is_err());
    }

    #[test]
    fn shutdown_timeout_is_bounded() {
        let mut config = ProcessExecutorConfig::default();
        config.shutdown_timeout = Duration::from_secs(60);
        assert!(config.validate().is_err());
    }

    #[test]
    fn configured_worker_binary_must_be_absolute() {
        let mut config = ProcessExecutorConfig::default();
        config.worker_binary = Some("./ezf-plugin-worker".into());
        assert!(config.validate().is_err());
    }

    #[test]
    fn datasource_port_list_is_bounded_and_rejects_zero() {
        let mut config = ProcessExecutorConfig::default();
        config.sandbox.datasource_tcp_ports = vec![0];
        assert!(config.validate().is_err());

        config.sandbox.datasource_tcp_ports = (1..=33).collect();
        assert!(config.validate().is_err());
    }

    #[test]
    fn per_plugin_sandbox_profile_keys_must_be_absolute() {
        let mut config = ProcessExecutorConfig::default();
        config.sandbox.profiles_by_plugin.insert(
            "relative/plugin.so".into(),
            PluginIpc::SandboxProfile::DomainPlugin
        );
        assert!(config.validate().is_err());
    }

    #[test]
    fn explicit_empty_per_plugin_mapping_is_preserved() {
        let mut config = ProcessExecutorConfig::default();
        config.sandbox.require_per_plugin_profile = true;
        assert!(config.sandbox.profiles_by_plugin.is_empty());
        assert!(config.validate().is_ok());
    }
}
