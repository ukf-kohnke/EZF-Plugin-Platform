# EZF Plugin Platform

[English documentation](README.md) · [Plugin-SDK](Plugin-SDK/README_DE.md) · [Plugin Test Host](PluginTestHost/README_DE.md) · [Plugin Worker](PluginWorker/README_DE.md)

`EZF-Plugin-Platform` ist der wiederverwendbare Rust-Workspace zum Hosten, Ausführen und Testen von EZF-Plugins. Er enthält die Plugin-Runtime, den ABI-Ausführungskern, das IPC-Protokoll für Prozess-Worker, das Worker-Binary, den eigenständigen Testhost und gemeinsam verwendete Filterverträge. Er ist nicht das EZF-Anwendungsbackend: HTTP-Routing, Authentifizierung, Anwendungskonfiguration, Deployment-Struktur und fachliche Use Cases liegen in der konsumierenden Anwendung und ihren Plugins.

Das verschachtelte Git-Submodul `Plugin-SDK` ist ein eigenständiger Cargo-Workspace und besitzt eigene `README.md`- und `README_DE.md`-Dateien. Diese Dokumentation beschreibt die Plattform um das SDK herum; sie ersetzt oder dupliziert bewusst nicht die SDK-Dokumentation.

## Aktueller Quellstand

| Merkmal | Aktueller Quellstand |
|---|---|
| Platform-Workspace-Version | `1.0.3` |
| Rust-Edition | `2024` |
| Cargo-Resolver | `2` |
| Plugin-ABI | ABI V2 (`InstantiateV2`, ABI-Version `2`) |
| Logischer Plugin-Vertrag | SDK-Vertrag `9`; der Host akzeptiert zusätzlich ältere Verträge `8` ohne `description` |
| Host/Worker-IPC | Protokoll `1.0` |
| Standardexecutor | `InProcess` |
| Worker-Paket / Binary | `PluginWorker` `1.0.1` / `ezf-plugin-worker` |
| Testhost-Paket / Binary | `PluginTestHost` / `ezf-plugin-test-host` |
| Verschachtelter SDK-Workspace | Version `1.0.2` |

Platform-Paketversion, SDK-Paketversion, Manifest-Vertragsversion und binäre ABI-Version beschreiben unterschiedliche Kompatibilitätsgrenzen. Sie dürfen nicht als eine gemeinsame Versionsnummer behandelt werden.

## Workspace-Komponenten

Die folgenden Crates sind Mitglieder des Root-`Cargo.toml`. `Plugin-SDK` ist ausdrücklich ausgeschlossen und wird als eigener Workspace gebaut und getestet.

| Crate | Paketversion | Verantwortung |
|---|---:|---|
| `PluginHost` | `1.0.3` | Explizite Host-Instanz, Plugin-Katalog/-Registry, Endpoint-Index, Ladepolitik, Lifecycle, Health, DataSource-Auswahl und Executor-Auswahl. |
| `PluginExecutionCore` | `1.0.3` | Gemeinsames Laden nativer Bibliotheken und ABI-V2-Aufrufe, ABI-Layoutprüfungen sowie Besitz und Freigabe von Response-Puffern. |
| `PluginIpc` | `1.0.3` | Versionierte IPC-Nachrichten, begrenztes Frame-Encoding/-Decoding, Request-Korrelation und Capability-Protokolltypen. Startet keine Prozesse und lädt keine Plugins. |
| `PluginWorker` | `1.0.1` | Prozess `ezf-plugin-worker`, der eine Plugin-Bibliothek lädt/aufruft und Host-vermittelte Capabilities bereitstellt. |
| `PluginTestHost` | `1.0.3` | CLI `ezf-plugin-test-host` zum Inspizieren, Aufrufen und szenariobasierten Testen eines Plugins ohne `BackendSrv`. |
| `FilterContracts` | `1.0.3` | Gemeinsame, datenbankunabhängige Filter-, Sortier-, Spalten- und Paginierungsstrukturen. |

### Grenzen der Komponenten

- **`PluginHost`** besitzt eine `PluginHostInstance`. Die Instanz kapselt Plugin-Root, Executor-Auswahl, Runtime-Limits, DataSource-Auswahl, Database-Capability-Callback, Katalog und Lifecycle-Zustand. Separate Instanzen teilen weder eine globale Registry noch einen globalen Datenbank-Callback.
- **`PluginExecutionCore`** ist die gemeinsame ABI-V2-Ausführungsschicht für dynamische Bibliotheken im In-Process-Host und im Worker. Sie hält die Bibliothek über die Plugin-Lebensdauer geladen und behandelt plugin-eigene Response-Puffer gemäß ABI-Vertrag.
- **`PluginIpc`** definiert das Worker-Drahtprotokoll. Aktuell ist dies Version `1.0`; der feste Header ist 36 Byte groß, der Control-Bereich ist standardmäßig auf 1 MiB und der Payload-Bereich auf 64 MiB begrenzt.
- **`PluginWorker`** läuft im Process-Modus außerhalb des Backend-Prozesses. Er lädt Plugins über `PluginExecutionCore` und vermittelt Datenbank-/Log-Aufrufe über IPC. Er stellt keine HTTP-Routen bereit und besitzt nicht den Plugin-Katalog des Hosts.
- **`PluginTestHost`** verwendet denselben Host-/Runtime-Pfad über eine CLI. Datenbankzugriffe können deterministisch mit Fixtures simuliert oder über ein separat geladenes reales `DataSource`-Plugin ausgeführt werden.
- **`FilterContracts`** ist ein neutrales Filter-/Query-Modell für Domain-Komponenten. Es gehört nicht zum `Plugin-SDK` und definiert weder Plugin-ABI noch Database-Capability-Vertrag.

## Runtime und Ausführungsmodi

Konsumierende Anwendungen erstellen eine `PluginHostConfig` und übergeben sie an `PluginHostInstance::new`. Die Host-Bibliothek liest bewusst weder Deployment-Umgebungsvariablen noch anwendungsspezifische Konfiguration. Die Anwendungskomposition (oder ein CLI-Adapter wie `PluginTestHost`) übersetzt ihre Einstellungen in die typisierte Host-Konfiguration.

`PluginHostConfig` enthält Plugin-Root, `PluginRuntimeConfig`, Executor-Auswahl (`InProcess` oder `Process`) und `ProcessExecutorConfig`.

| Modus | Ausführung | Aktuelle Einschränkungen |
|---|---|---|
| `InProcess` | Plugin-Bibliothek wird im Host-Prozess über `PluginExecutionCore` geladen und aufgerufen. | Plugin-Aufrufe können innerhalb der Worker- und Admission-Limits parallel laufen. Panics/Fehler werden an der Runtime-Grenze behandelt; native Plugins teilen sich aber weiterhin den Host-Prozess. |
| `Process` | Ein Worker-Prozess lädt das Plugin und tauscht Requests über `PluginIpc` mit dem Host aus. | Der aktuelle Transport verwendet Unix-Domain-Sockets und wird auf Unix-Zielen unterstützt. Der IPC-Aufrufpfad eines Workers arbeitet derzeit Single-Flight; mehr Host-Worker-Threads erzeugen daher keine parallelen Aufrufe innerhalb dieses Workers. |

Die aktuellen Defaults von `PluginRuntimeConfig` sind 30 Sekunden für Endpoint-Aufrufe, 10 Sekunden für Unload, 10 Sekunden für Lifecycle-Operationen, 16 persistente Worker-Threads, 32 zugelassene Aufrufe und Queue-Kapazität 24 pro Plugin. Dies sind Defaults, keine universelle Dimensionierungsempfehlung. Der Process-Executor verwendet standardmäßig 500 ms Cancellation-Grace und 2 Sekunden Worker-Shutdown-Timeout; beide Werte werden begrenzt validiert.

Der Host wählt die Database Capability aus den geladenen Plugins mit Manifest-Typ `DataSource`. Anwendungen, die Datenbankzugriffe benötigen, müssen sicherstellen, dass genau eine auswählbare DataSource geladen ist. Domain-Plugins verwenden die Database-Capability des SDK und linken nicht gegen ein konkretes Datenbank-Plugin.

## Process-Worker und Sandbox

Der Process-Modus startet `ezf-plugin-worker`. `ProcessExecutorConfig` enthält den optionalen absoluten Pfad zum Worker-Binary, Sandbox-Einstellungen, eine explizite Worker-Umgebung sowie Cancellation-Grace und Shutdown-Timeout. `PluginHost` leitet diese Werte nicht aus EZF-spezifischen Umgebungsvariablen ab.

Der Linux-Worker unterstützt aktuell erzwungene Profile auf Linux `x86_64` und `aarch64`:

- `DomainPlugin`: Landlock-Allowlist für schreibgeschützte Dateisystempfade, privates beschreibbares Plugin-Temp-Verzeichnis, seccomp-Einschränkungen, `no_new_privs`, Capability-Prüfungen, Reduzierung geerbter Dateideskriptoren und ausgewählte `RLIMIT`-Grenzen. Datenbankzugriff muss über den Host-Capability-Broker erfolgen.
- `DataSourcePlugin`: dieselben Basiseinschränkungen plus Landlock-Regeln für TCP-Zielports. Portregeln sind keine IP-Allowlist. Firewall-/Container-/Netzwerkregeln müssen Ziel-IPs und Nicht-TCP-Egress separat beschränken; der Worker kann nur prüfen, ob der Betreiber diese externe Kontrolle bestätigt hat.
- `DevelopmentUnconfined`: expliziter Entwicklungs-/Testmodus ohne OS-Sandbox. Dies ist keine produktive Isolationsgrenze.

Der erzwungene Modus bricht geschlossen ab, wenn Betriebssystem oder Kernel die benötigten Primitive nicht bereitstellen. DataSource-Profile benötigen Landlock ABI 4 oder neuer. Der Worker erstellt keine cgroups und keinen User-Namespace, wechselt nicht die UID und erzwingt kein Gesamtlimit für temporären Speicher; dies bleibt Aufgabe des Deployments. Die exakten Umgebungsvariablen, Limits und unterstützten Kontrollen stehen in der [Worker-Dokumentation](PluginWorker/README_DE.md).

## Eigenständiger Plugin-Testhost

Die CLI wird aus dem Platform-Workspace gebaut und ausgeführt:

```bash
cargo build -p PluginTestHost -p PluginWorker
cargo test -p PluginTestHost

# Kompilierte Plugin-Bibliothek inspizieren
cargo run -p PluginTestHost -- inspect /absoluter/pfad/libmy_plugin.so

# JSON-Szenario ausführen
cargo run -p PluginTestHost -- test /absoluter/pfad/libmy_plugin.so \
  --scenario PluginTestHost/examples/scenario.json \
  --executor in-process \
  --output target/plugin-test-report.json
```

Die CLI unterstützt außerdem einen einzelnen `call`, Prozessausführung, deterministische Datenbank-Mock-Regeln und ein explizit ausgewähltes Live-DataSource-Plugin. Sie erzeugt einen maschinenlesbaren JSON-Report und verwendet Exit-Codes `0` (alle Assertions erfolgreich), `1` (Test-/Lifecycle-Assertion fehlgeschlagen) und `2` (ungültige Eingabe oder Setup-Fehler). Ein Mock-Zustand gilt pro Testhost-Prozess; eine Szenario-Suite wird sequenziell gegen eine Plugin-Instanz ausgeführt. Das vollständige Szenario-Schema und Beispiele für Live-Datenbankzugriff stehen in der [Testhost-Dokumentation](PluginTestHost/README_DE.md).

## Build und Tests

Die folgenden Befehle werden im Root dieses Repositories (`EZF-Plugin-Platform`) ausgeführt:

```bash
# Alle Platform-Workspace-Mitglieder bauen
cargo build --workspace

# Alle Workspace-Tests ausführen
cargo test --workspace

# Host/Runtime und Worker gezielt testen
cargo test -p PluginHost
cargo test -p PluginWorker -- --test-threads=1
cargo test -p PluginIpc
cargo test -p PluginTestHost
```

`Plugin-SDK` ist von diesem Workspace ausgeschlossen. Build und Tests des SDK werden gemäß dessen [deutscher](Plugin-SDK/README_DE.md) oder [englischer](Plugin-SDK/README.md) README aus `Plugin-SDK/` ausgeführt. Platform-Tests mit einer echten Plugin-Bibliothek benötigen eine für dasselbe Betriebssystem/dieselbe Architektur gebaute Bibliothek mit kompatiblem ABI-V2-Vertrag. Tests mit Live-DataSource benötigen zusätzlich deren pluginlokale Konfiguration und eine erreichbare Datenbank.

## Beziehung zum EZF-Backend

Im EZF-Backend ist dieses Repository als Git-Submodul `Plugin-Platform` ausgecheckt. Der Root-Cargo-Workspace des Backends schließt den Platform-Root-Workspace aus, enthält aber ausgewählte Pakete (insbesondere `PluginWorker`) bei Bedarf in seinem eigenen Build. `PluginTestHost` bleibt Mitglied des separaten Platform-Workspaces und muss aus dem Backend-Root mit `--manifest-path Plugin-Platform/Cargo.toml` gebaut werden.

Das Backend liefert Anwendungsaspekte wie Rocket-Routen, Authentifizierung, `ApplicationConfig`, Deployment-Umgebungsvariablen und die konkrete `MolgenDB`-DataSource. Die Platform liefert Host/Runtime, Worker und Testhost als wiederverwendbare Komponenten. Für die Backend-Integration und die tatsächlich bereitgestellten APIs ist die README des EZF-Backend-Repositories maßgeblich, wenn dieses Submodul dort ausgecheckt ist.

## Weiterführende Dokumentation

- [Konfiguration des Plugin-Host-Process-Executors](PluginHost/docs/process-executor-configuration.md)
- [Plugin Worker](PluginWorker/README_DE.md) · [Englisch](PluginWorker/README.md)
- [Plugin Test Host](PluginTestHost/README_DE.md) · [Englisch](PluginTestHost/README.md)
- [Plugin-SDK](Plugin-SDK/README_DE.md) · [Englisch](Plugin-SDK/README.md)
