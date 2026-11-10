# EZF Plugin Test Host

[English documentation](README.md) · [Platform-Übersicht](../README_DE.md)

`ezf-plugin-test-host` ist ein eigenständiges CLI zum Inspizieren, Aufrufen und szenariobasierten Testen einer Plugin-Shared-Library, ohne `BackendSrv` zu starten. Es verwendet `PluginHost` und dieselben ABI-V2-/Executor-Pfade wie die Anwendung. Datenbankaufrufe können durch deterministische Fixtures simuliert oder explizit an ein reales DataSource-Plugin (z. B. `MolgenDB`) weitergeleitet werden.

## Bauen

Aus dem Root des Platform-Workspaces:

```bash
cargo build -p PluginTestHost -p PluginWorker
cargo test -p PluginTestHost
```

Das Worker-Binary wird standardmäßig neben `ezf-plugin-test-host` gesucht. Bei einem abweichenden Ort kann `EZF_PLUGIN_WORKER_PATH` auf den absoluten Pfad zu `ezf-plugin-worker` zeigen.

## Plugin inspizieren und einzelnen Request ausführen

```bash
cargo run -p PluginTestHost -- inspect ./target/debug/libmy_plugin.so
cargo run -p PluginTestHost -- call ./target/debug/libmy_plugin.so \
  --request PluginTestHost/examples/request.json --executor in-process
```

Alle CLI-Ausgaben sind JSON. `--output report.json` schreibt denselben JSON-Report zusätzlich in eine Datei.

## Process-Modus

```bash
cargo build -p PluginTestHost -p PluginWorker
# Nur lokale Entwicklung, wenn kein erzwungenes Sandbox-Profil konfiguriert ist:
EZF_PLUGIN_ALLOW_UNCONFINED=1 cargo run -p PluginTestHost -- test \
  ./target/debug/libmy_plugin.so \
  --scenario PluginTestHost/examples/scenario.json \
  --executor process --output target/plugin-test-report.json
```

Der Testhost deaktiviert Sandbox-Prüfungen niemals implizit. Unter Linux ist für eine erzwungene Ausführung ein geeignetes `EZF_PLUGIN_SANDBOX_PROFILE` (oder `EZF_PLUGIN_SANDBOX_PROFILES`) zu konfigurieren. `EZF_PLUGIN_ALLOW_UNCONFINED=1` darf nur für Entwicklung und Tests ausdrücklich gesetzt werden. Der aktuelle Process-Executor unterstützt ausschließlich Unix-Domain-Sockets. Ein unconfined Worker ist nicht durch eine OS-Sandbox isoliert.

## Szenarioformat

Eine Testsuite ist ein einzelnes JSON-Dokument mit folgenden Top-Level-Feldern:

- `name`: optionales Label für den Report.
- `executor`: `in_process` oder `process`; `--executor` auf der CLI überschreibt den Wert.
- `timeouts`: optionale `call_ms`, `unload_ms` und `lifecycle_ms`; CLI-Timeout-Flags haben Vorrang.
- `principal`: Standard-Session-/Benutzerkontext für Requests ohne request-spezifischen `principal`.
- `database`: Datenbankmodus und im Mock-Modus deterministische Fixtures. Ohne `mode` bzw. mit `"mode": "mock"` werden Fixtures verwendet. `"mode": "live"` leitet Datenbankaufrufe an ein reales DataSource-Plugin weiter.
- `tests`: nichtleere, geordnete Liste von Testfällen.
- `expect_unload_success`: erwartetes Ergebnis des abschließenden Unload (Standard `true`).

Ein Testfall kann enthalten:

- `request`: Endpoint-Request mit `endpoint_id`, `method`, `path`, optional `base_path`, `raw_query`, `content_type`, JSON-`body` und `principal`.
- `session_closed: true` (alternativ `notify_session_closed`), um den Session-Close-Callback auszulösen. `session_id` wird explizit angegeben oder aus dem Request-/Suite-Principal übernommen.
- `expect`: optionale Assertions `status`, `body_equals`, `error_contains`, `session_close_success`, `health_state` und `database_call_count`.

Ein Request-Timeout ist ein testbares Ergebnis und nicht automatisch ein CLI-Fehler. Mit `status` und/oder `error_contains` lässt sich der erwartete Timeout prüfen. Der Report enthält vor dem Unload außerdem den Health-Snapshot mit aktiven und zeitüberschrittenen Aufrufen.

## Datenbank-Mocks

`database` unterstützt:

- `default_query`: ein `DatabaseContracts::ResultSet` (Standard: leere Rows).
- `default_execute_rows_affected`: Standardanzahl betroffener Zeilen (Standard `1`).
- `default_error`: optionaler Fehler `{ "kind": "query", "message": "..." }`, wenn keine Regel greift.
- `rules`: geordnete Regeln. Jede Regel benötigt eine Match-Bedingung (`operation`, `statement_contains` oder `call_number`) und genau eines der Felder `response` oder `error`.

SQL-Substring-Matches sind case-sensitive, Operationsnamen werden case-insensitive verglichen. `call_number` beginnt bei 1 und zählt über die aktuelle Suite. Responses folgen dem serialisierten `DatabaseResponse`-Vertrag, beispielsweise `{ "result": "query", "rows": [] }` oder `{ "result": "execute", "rows_affected": 2 }`.

Der Database-ABI-V2-Callback besitzt keinen eigenen User-Data-Parameter pro Callback. Deshalb hält diese CLI genau einen explizit konfigurierten Mock-Zustand pro Testhost-Prozess und führt eine Suite sequenziell gegen ein Plugin aus. Dieser Zustand ist weder global für `PluginHost` noch für `BackendSrv`; jeder CLI-Aufruf ist ein eigener Prozess.

## Live-SQL-Server-/MolgenDB-Modus

Um dieselbe SQL-Server-Konfiguration wie ein laufendes Backend zu verwenden, lädt der Testhost das bereitgestellte `MolgenDB`-DataSource-Plugin separat. Shared Library, `config.json` und `database.ini` müssen im selben Deployment-Verzeichnis liegen. Das Szenario enthält keine Credentials oder Connection Strings; der Pfad zur DataSource-Bibliothek wird explizit angegeben:

```json
"database": { "mode": "live" }
```

Beispiel für ein macOS-Debug-Deployment:

```bash
cargo build -p PluginTestHost -p PluginWorker
EZF_PLUGIN_ALLOW_UNCONFINED=1 cargo run -p PluginTestHost -- test \
  ./deployment/debug/plugins/libVariantFilter.dylib \
  --scenario PluginTestHost/examples/scenario-variantfilter-live-db-smoke.json \
  --database-plugin ./deployment/debug/plugins/database/MolgenDB/libMolgenDB.dylib \
  --executor process \
  --output target/variantfilter-live-db-report.json
```

`EZF_PLUGIN_ALLOW_UNCONFINED=1` nur für lokale Entwicklung verwenden, wenn kein erzwungenes Sandbox-Profil eingerichtet ist. Bei aktivem Profil die Variable weglassen. Bibliotheksnamen und Endungen an das tatsächliche Betriebssystem/Deployment anpassen. `--database-plugin` muss auf die bereitgestellte Bibliothek zeigen, nicht lediglich auf `target/debug/libMolgenDB.dylib`, sofern dort nicht auch die passenden `config.json`- und `database.ini`-Dateien vorhanden sind.

Der Testhost führt einen Readiness-Probeaufruf (`SELECT 1`) aus und leitet die Datenbank-Requests des Domain-Plugins über ABI V2 an das DataSource-Plugin weiter. Nach der Testsuite entlädt er die DataSource. Dabei wird ein **eigener Connection-Pool** verwendet; der Testhost teilt nicht die aktive SQL-Verbindung oder den Pool eines bereits laufenden Backends. Es wird nur dieselbe SQL-Server-/Datenbankkonfiguration angesprochen.

`database.mode = "live"` kann nicht mit `--database-fixtures` oder Mock-Regeln kombiniert werden. Die Endpoint-ID von VariantFilter lautet `load-snv-data`; der HTTP-Aufruf ist jedoch `POST /loadSnvData` (nicht `/load-snv-data`). Der Body benötigt eine oder mehrere eindeutige Patienten-UUIDs:

```json
{
  "endpoint_id": "load-snv-data",
  "method": "POST",
  "path": "/loadSnvData",
  "body": { "patient_ids": ["REPLACE-WITH-A-REAL-PATIENT-UUID"] }
}
```

Zwei Szenarien sind vorbereitet:

- `examples/scenario-variantfilter-live-db-smoke.json` prüft einen echten SQL-Request und HTTP 200. Wenn für die Beispiel-UUID keine Analyse vorhanden ist, kann die Antwort `status: "not_loaded"` enthalten. Es ist daher ein Verbindungs-/Query-Smoke-Test ohne bekannte Patienten-ID.
- `examples/scenario-variantfilter-load-snv-data.json` verlangt `status: "loaded"`, ruft danach `GET /getSnvCache` in derselben Session auf und prüft den Cache. Ersetze die Beispiel-UUID durch eine reale Patienten-ID aus der Testdatenbank, deren ausgewählte Analyse SNV-Varianten enthält.

Mögliche IDs lassen sich über `examples/variantfilter-find-patient-with-snvs.sql` ermitteln. Die SQL-Abfrage spiegelt die aktuelle Analyseauswahl von VariantFilter (`TOP (1) ... ORDER BY a.id`). Nur mit einer geeigneten Testdatenbank ausführen. Obwohl der Endpoint eine Read-Abfrage sendet, legt er das geladene Ergebnis bis zum Session-Close/Unload im In-Memory-Session-Cache des Plugins ab.

## JSON-Reports und Exit-Codes

Der `test`-Befehl gibt immer einen JSON-Report mit `schema_version: 1` aus. Er enthält Suite-Konfiguration, Setup, Assertions je Testfall, Datenbank-Requests/-Ergebnisse, Runtime-Health, Unload-Ergebnis und Zusammenfassung. `--output <path>` schreibt dasselbe Dokument in eine Datei.

- `0`: Alle Testfälle und der abschließende Lifecycle-Check waren erfolgreich.
- `1`: Mindestens eine Test-Assertion oder Lifecycle-Erwartung ist fehlgeschlagen.
- `2`: Ungültige CLI-/Konfigurationseingabe oder Setup-Fehler bei `inspect`/`call`.

Für CI ist der JSON-Report die stabile Ergebnisschnittstelle; normale stdout-Diagnosemeldungen sind nicht als Maschinenformat gedacht.
