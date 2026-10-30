//! PluginTestHost-owned environment adapter for the explicit process runtime configuration.
//!
//! Keeping this translation in the CLI means PluginHost/ProcessExecutor itself has no knowledge
//! of EZF_PLUGIN_* deployment variable names.

use PluginHost::PluginHostInstance::{ ProcessExecutorConfig, SandboxProfile };
use std::{ collections::BTreeMap, path::PathBuf, time::Duration };

pub(crate) fn from_environment(enabled: bool) -> Result<ProcessExecutorConfig, String> {
    if !enabled {
        return Ok(ProcessExecutorConfig::default());
    }

    let mut config = ProcessExecutorConfig::default();
    config.worker_binary = std::env::var_os("EZF_PLUGIN_WORKER_PATH").map(PathBuf::from);
    config.cancellation_grace = duration(
        "EZF_PLUGIN_PROCESS_CANCEL_GRACE_MS",
        Duration::from_millis(500),
        Duration::from_millis(10),
        Duration::from_secs(30)
    )?;
    config.shutdown_timeout = duration(
        "EZF_PLUGIN_PROCESS_SHUTDOWN_TIMEOUT_MS",
        Duration::from_secs(2),
        Duration::from_millis(100),
        Duration::from_secs(15)
    )?;

    config.sandbox.default_profile = match std::env::var("EZF_PLUGIN_SANDBOX_PROFILE") {
        Ok(value) => Some(profile(&value)?),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => {
            return Err(format!("could not read EZF_PLUGIN_SANDBOX_PROFILE: {error}"));
        }
    };
    if let Ok(raw) = std::env::var("EZF_PLUGIN_SANDBOX_PROFILES") {
        config.sandbox.require_per_plugin_profile = true;
        let profiles = serde_json
            ::from_str::<BTreeMap<String, String>>(&raw)
            .map_err(|error| format!("invalid EZF_PLUGIN_SANDBOX_PROFILES JSON: {error}"))?;
        for (path, value) in profiles {
            config.sandbox.profiles_by_plugin.insert(PathBuf::from(path), profile(&value)?);
        }
    }

    config.sandbox.extra_read_paths = paths("EZF_PLUGIN_SANDBOX_READ_PATHS");
    config.sandbox.library_paths = paths("EZF_PLUGIN_SANDBOX_LIBRARY_PATHS");
    config.sandbox.datasource_egress_policy_acknowledged = match
        std::env::var("EZF_PLUGIN_DATASOURCE_EGRESS_POLICY")
    {
        Ok(value) => value.trim().eq_ignore_ascii_case("externally-enforced"),
        Err(std::env::VarError::NotPresent) => false,
        Err(error) => {
            return Err(format!("could not read EZF_PLUGIN_DATASOURCE_EGRESS_POLICY: {error}"));
        }
    };
    if let Ok(raw) = std::env::var("EZF_PLUGIN_DATASOURCE_ALLOWED_TCP_PORTS") {
        for item in raw
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty()) {
            let port = item
                .parse::<u16>()
                .map_err(|_| format!("invalid DataSource TCP port {item:?}; expected 1..65535"))?;
            if port == 0 {
                return Err(format!("invalid DataSource TCP port {item:?}; expected 1..65535"));
            }
            config.sandbox.datasource_tcp_ports.push(port);
        }
    }
    config.sandbox.allow_unconfined = match std::env::var("EZF_PLUGIN_ALLOW_UNCONFINED") {
        Ok(value) => parse_bool("EZF_PLUGIN_ALLOW_UNCONFINED", &value)?,
        Err(std::env::VarError::NotPresent) => false,
        Err(error) => {
            return Err(format!("could not read EZF_PLUGIN_ALLOW_UNCONFINED: {error}"));
        }
    };

    if let Ok(names) = std::env::var("EZF_PLUGIN_WORKER_ENV_ALLOWLIST") {
        for name in names
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty()) {
            if let Some(value) = std::env::var_os(name) {
                config.worker_environment.variables.insert(name.to_owned(), value);
            }
        }
    }

    config.validate()?;
    Ok(config)
}

fn duration(
    name: &str,
    default: Duration,
    min: Duration,
    max: Duration
) -> Result<Duration, String> {
    let raw = match std::env::var(name) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => {
            return Ok(default);
        }
        Err(error) => {
            return Err(format!("could not read {name}: {error}"));
        }
    };
    let millis = raw
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("{name} must be an integer number of milliseconds"))?;
    let value = Duration::from_millis(millis);
    if value < min || value > max {
        return Err(format!("{name} must be between {min:?} and {max:?}"));
    }
    Ok(value)
}

fn paths(name: &str) -> Vec<PathBuf> {
    std::env
        ::var_os(name)
        .map(|value|
            std::env
                ::split_paths(&value)
                .filter(|path| !path.as_os_str().is_empty())
                .collect()
        )
        .unwrap_or_default()
}

fn profile(value: &str) -> Result<SandboxProfile, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "domain_plugin" | "domain" => Ok(SandboxProfile::DomainPlugin),
        "data_source_plugin" | "datasource" | "data_source" => Ok(SandboxProfile::DataSourcePlugin),
        "development_unconfined" | "unconfined" => Ok(SandboxProfile::DevelopmentUnconfined),
        other => Err(format!("unsupported plugin sandbox profile {other:?}")),
    }
}

fn parse_bool(name: &str, value: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        other => Err(format!("{name} must be true/false or 1/0, got {other:?}")),
    }
}
