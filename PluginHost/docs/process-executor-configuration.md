# ProcessExecutor-Konfiguration

Der `ProcessExecutor` liest keine EZF-Deployment-Umgebungsvariablen. Sämtliche Launch-Parameter
werden über `PluginHostConfig::process_executor` an die Runtime übergeben.

```rust
use PluginHost::PluginHostInstance::{
    PluginExecutorSelection, PluginHostConfig, PluginHostInstance,
    PluginRuntimeConfig, ProcessExecutorConfig, SandboxConfig, SandboxProfile,
};
use std::{path::PathBuf, time::Duration};

let mut process = ProcessExecutorConfig::default();
process.worker_binary = Some(PathBuf::from("/absolute/path/to/ezf-plugin-worker"));
process.cancellation_grace = Duration::from_millis(500);
process.shutdown_timeout = Duration::from_secs(2);
process.sandbox = SandboxConfig {
    default_profile: Some(SandboxProfile::DomainPlugin),
    ..SandboxConfig::default()
};

let host = PluginHostInstance::new(PluginHostConfig {
    plugin_root: PathBuf::from("/absolute/path/to/plugins"),
    runtime: PluginRuntimeConfig::default(),
    executor: PluginExecutorSelection::Process,
    process_executor: process,
});
```

## Konfigurationsfelder

- `worker_binary`: Optionaler absoluter Pfad zum `ezf-plugin-worker`. `None` sucht das Worker-Binary
  neben dem Host-Executable bzw. im Cargo-Testlayout unter `target/<profile>`.
- `sandbox.default_profile`: Standardprofil (`DomainPlugin`, `DataSourcePlugin` oder
  `DevelopmentUnconfined`).
- `sandbox.profiles_by_plugin`: optionale Profile pro kanonischem absolutem Plugin-Pfad. Ist die Map
  nicht leer, muss jeder Prozessmodus-Pluginpfad darin vorhanden sein.
- `sandbox.require_per_plugin_profile`: erzwingt die per-Plugin-Zuordnung auch dann, wenn die Map
  leer ist.
- `sandbox.extra_read_paths`: zusätzliche, eng begrenzte Dateisystem-Lesepfade.
- `sandbox.library_paths`: vertrauenswürdige dynamische Library-Suchpfade; sie werden ebenfalls als
  Lesepfade freigegeben und müssen sichere Verzeichnisrechte besitzen.
- `sandbox.datasource_tcp_ports`: erlaubte Zielports für DataSource-Worker.
- `sandbox.datasource_egress_policy_acknowledged`: bestätigt, dass eine Firewall oder Container-Policy
  die Ziel-IP-Beschränkung außerhalb von seccomp erzwingt.
- `sandbox.allow_unconfined`: explizite Freigabe des Development-only-Modus ohne OS-Sandbox.
- `worker_environment.variables`: explizite Worker-Umgebungsvariablen. Die Runtime validiert Namen
  und verbietet Loader-/Runtime-Steuerungsvariablen; für Domain-Plugins werden zusätzlich
  secret-/datenbankähnliche Variablennamen abgelehnt.
- `cancellation_grace` und `shutdown_timeout`: Zeitfenster für Cancellation- und Shutdown-Eskalation.

Die Zeitbudgets werden auf 10 ms bis 30 s bzw. 100 ms bis 15 s begrenzt. DataSource-Worker benötigen
zusätzlich eine nichtleere TCP-Portliste (1 bis 32 Ports) und die externe Egress-Policy-Bestätigung.

## Environment-Übersetzung

Die Anwendung darf weiterhin Umgebungsvariablen als Deployment-Konfiguration verwenden. Sie liest
und validiert diese aber an ihrer Composition Root (im EZF-Backend beispielsweise
`BackendSrv::plugin_host_config`) und übergibt danach `ProcessExecutorConfig` explizit an den Host.
`PluginTestHost` besitzt eine eigene CLI-seitige Konfigurationsübersetzung. Änderungen an der
Prozessumgebung nach Erzeugung einer Host-Instanz beeinflussen deren Konfiguration nicht.
