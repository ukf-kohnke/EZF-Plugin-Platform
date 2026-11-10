# ezf-plugin-worker

[English documentation](README.md) · [Platform-Übersicht](../README_DE.md)

`ezf-plugin-worker` ist die prozessisolierte Runtime für eine Plugin-Shared-Library. Sie verwendet ABI V2 und `PluginExecutionCore`, spricht das begrenzte `PluginIpc`-Protokoll und leitet Datenbank-/Logging-Capabilities an den Host-Broker weiter. Der Worker hostet weder die HTTP-API des Backends noch besitzt er die Plugin-Registry.

## Entwicklungsmodus ohne OS-Sandbox

Lokale Integrationstests können den Worker ausdrücklich ohne erzwungene Sandbox starten:

```bash
ezf-plugin-worker --ipc-socket /absoluter/pfad/zu/private.sock --allow-unconfined
```

Der Host muss zusätzlich `EZF_PLUGIN_ALLOW_UNCONFINED=1` freigeben. Dieser Modus meldet `sandbox_enforced = false` und stellt keine produktive Sicherheitsgrenze dar.

## Erzwungene Linux-Profile

Erzwungene Profile werden auf Linux `x86_64` und `aarch64` unterstützt. Der Worker richtet die Sandbox vor dem Laden von Plugin-Code ein, meldet aktive bzw. fehlende Kontrollen über `SandboxReady` und beendet sich bei einem Setup-Fehler, statt still auf ungeschützte Ausführung zurückzufallen.

- `DomainPlugin`: Landlock-Allowlist für schreibgeschützte Dateisystempfade, privates beschreibbares Plugin-Temp-Verzeichnis, seccomp-Einschränkungen für direkte Netzwerkzugriffe, Prozess-Erzeugung, Privileg-/Capability-Änderungen und Cross-Process-Signale, `no_new_privs`, Capability-Prüfung, Reduzierung geerbter Dateideskriptoren und `RLIMIT`-Grenzen. Datenbankzugriff muss über den Host-Capability-Broker laufen.
- `DataSourcePlugin`: dieselben Dateisystem-/Prozess-Einschränkungen sowie Landlock-Allowlist für TCP-Zielports. Die IP-basierte Egress-Allowlist und Einschränkungen für Nicht-TCP-Verkehr **müssen durch Firewall, Container oder Netzwerk-Namespace des Deployments** erzwungen werden. Der Worker prüft nur, ob der Betreiber diese Anforderung bestätigt hat; die Firewall-Regeln kann er nicht verifizieren.
- Erzwungene Profile werden auf Nicht-Linux-Systemen abgelehnt. Ein Linux-Kernel ohne benötigte Landlock-ABI oder seccomp-Unterstützung wird ebenfalls abgelehnt. DataSource benötigt für TCP-Portregeln Landlock ABI 4 oder neuer.

Der Worker muss als Nicht-Root-Benutzer und mit leeren Linux-Capability-Sets laufen. Dafür sollte eine dedizierte Worker-Identität bzw. ein User-Namespace des Deployments verwendet werden, sofern möglich. Der Worker meldet außerdem Deployment-Anforderungen für cgroup-CPU-/Speichergrenzen und ein Gesamtlimit für das Temp-Dateisystem. `RLIMIT_FSIZE` begrenzt jede einzelne Datei, nicht den gesamten Temp-Speicher. Diese externen Kontrollen gelten daher nicht automatisch als aktiv.

## Host-Konfiguration

Der Process-Modus wird ausdrücklich aktiviert. Wenn Domain- und DataSource-Plugins gemeinsam geladen werden, ist eine Profilzuordnung pro Plugin zu verwenden. Die Schlüssel müssen dem kanonischen absoluten Pfad der Shared-Library entsprechen:

```bash
export EZF_PLUGIN_EXECUTOR=process
export EZF_PLUGIN_WORKER_PATH=/absoluter/pfad/zu/ezf-plugin-worker
export EZF_PLUGIN_SANDBOX_PROFILES='{"/opt/ezf/plugins/VariantFilter/libvariant_filter.so":"domain_plugin","/opt/ezf/plugins/MolgenDB/libmolgendb.so":"data_source_plugin"}'
```

Für eine Entwicklungs-/Staging-Installation, die ausschließlich Domain-Plugins lädt, kann statt der Map `EZF_PLUGIN_SANDBOX_PROFILE=domain_plugin` verwendet werden. `EZF_PLUGIN_ALLOW_UNCONFINED=1` ist auf lokale Entwicklung zu beschränken.

Der Dateisystem- und Bibliothekszugriff des Workers ist ausdrücklich zu konfigurieren:

- `EZF_PLUGIN_SANDBOX_READ_PATHS`: durch das Betriebssystem getrennte Liste konkreter Konfigurationsdateien oder eng begrenzter Verzeichnisse. Pfade werden schreibgeschützt freigegeben und erhalten nicht automatisch Rechte zum Laden dynamischer Bibliotheken. `/`, `/proc`, `/sys`, `/dev`, `/run` sowie breite `/etc`, `/home`, `/root`, `/var` und `/tmp`-Wurzeln werden abgelehnt.
- `EZF_PLUGIN_SANDBOX_LIBRARY_PATHS`: Liste vertrauenswürdiger dynamischer Bibliotheksverzeichnisse, die nicht für Gruppe/Welt beschreibbar sein dürfen. Nur diese Pfade werden zum Aufbau von `LD_LIBRARY_PATH` verwendet; geerbte Loader-Overrides werden nicht übernommen.
- `EZF_PLUGIN_WORKER_ENV_ALLOWLIST`: kommaseparierte Liste explizit zu übergebender Umgebungsvariablen. Loader-Overrides, `EZF_PLUGIN_*`, `PATH`, `HOME`, `TMPDIR`, `TMP`, `TEMP` und `RUSTFLAGS` sind verboten. Secret-/Datenbank-ähnliche Namen werden für Domain-Plugins ebenfalls abgelehnt. Der Host setzt `HOME` und `TMPDIR` auf das private Temp-Verzeichnis. DataSource-Secrets sollen nur für dieses Profil und nur bei betrieblicher Notwendigkeit freigegeben werden.

Für DataSource-Plugins:

```bash
export EZF_PLUGIN_DATASOURCE_EGRESS_POLICY=externally-enforced
export EZF_PLUGIN_DATASOURCE_ALLOWED_TCP_PORTS=1433
```

Die TCP-Portliste wird durch Landlock (ABI 4+) erzwungen, stellt aber keine IP-Allowlist dar. Firewall- oder Container-Regeln müssen sämtlichen erlaubten Netzwerkverkehr auf das Datenbankziel und benötigte DNS-Resolver beschränken. Der Host protokolliert diese Kontrolle als deploymentverwaltet. Wenn sich die Egress-Policy nicht garantieren lässt, sollte die DataSource in einem separaten Container/Service oder über einen DB-Broker betrieben werden.

## Ressourcen- und Isolationsgrenzen

Der Worker setzt `RLIMIT_CORE=0`, `RLIMIT_NOFILE=128` und `RLIMIT_FSIZE=64 MiB` (jeweils nur, wenn dadurch keine bereits strengere Grenze gelockert wird), aktiviert `PR_SET_DUMPABLE=0` und `PR_SET_NO_NEW_PRIVS=1` und schließt geerbte Dateideskriptoren bis auf Standardkanäle und IPC-Verbindungen.

Der Worker richtet keine cgroups ein, erzeugt keinen User-Namespace, ändert nicht die UID, mountet kein privates Dateisystem und setzt kein Gesamtlimit für temporären Speicher. Diese Kontrollen müssen vom Deployment bereitgestellt werden und bleiben in `SandboxReady.missing_features` sichtbar.

Landlock und seccomp sind Defense-in-Depth und keine vollständige virtuelle Maschine. Insbesondere ist die generische SQL-Policy des Host-Capability-Brokers eine separate Autorisierungsgrenze. `sandbox_enforced = true` allein beweist nicht, dass Firewall-, cgroup-, UID-/User-Namespace- und Temp-Quota-Kontrollen aktiv sind.

## Integrationstests

Aus dem Root des `EZF-Plugin-Platform`-Workspaces:

```bash
cargo test -p PluginWorker --test process_integration -- --test-threads=1
cargo test -p PluginHost --test dynamic_library_loader -- --test-threads=1
```

Der Linux-Integrationstest prüft entweder, dass Netzwerk-, Datei-, Prozess-, Signal- und Capability-Operationen blockiert und private Temp-Schreibzugriffe erlaubt werden, oder dass der Worker bei fehlender Kernel-Funktion geschlossen abbricht. Die Tests sollten auf jeder unterstützten Linux-Architektur und in der Zielumgebung ausgeführt werden.
