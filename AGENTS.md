# AGENTS.md — Lakehouse-Projekt

Dieses Dokument gibt Agents (auch leichteren Modellen) den Kontext, um im
Repository sinnvolle Vorschläge zu machen, ohne die Architektur-Geschichte
des Projekts neu ableiten zu müssen. Lies diesen Abschnitt vor jedem Task.

`CLAUDE.md` im Repo-Root verweist ausschließlich hierher — diese Datei ist die
Single Source of Truth für den Agenten-Kontext, unabhängig vom verwendeten Tool.

### Repo-Charakter

Dies ist ein **Design- und Deployment-Config-Repository, kein Anwendungs-Code**:
Markdown-Konzepte, Helm-Values, K8s-ConfigMaps/Manifeste, Argo-`WorkflowTemplate`/
`CronWorkflow`-YAML, SQL und ein einzelner PySpark-Job. Es gibt **keinen Build,
kein Lint, keine Test-Suite**. "Ausführen" heißt: auf einen Kubernetes-Cluster
applien (`kubectl` / `helm` / `argo`); die vollständigen Deployment-Sequenzen
stehen in den `README.md` der jeweiligen Subfolder. Repo-Sprache ist Deutsch —
neue Doku und Kommentare ebenso.

Dem "Single Test" am nächsten kommt ein Dry-Run des Maintenance-Jobs:
`argo submit --from cronwf/iceberg-daily-maintenance -n argo -p dry-run=true -p namespaces=gold`
(oder `maintenance.py --dry-run` direkt via `spark-submit`).

---

## 1. Projektkontext (in 6 Zeilen)

Unternehmensinternes Lakehouse:

- **Volumen**: 100 TB – 1 PB, mehrere TB/Tag Wachstum
- **Nutzer**: ausschließlich intern (BI + Customer-facing Dashboards für interne Stakeholder + ML/DS); **kein echtes Multi-Tenancy**
- **SLA**: Sub-Second für Customer-facing Dashboards, > 100 QPS, > 500 parallele User
- **Storage**: S3-kompatibel via MinIO (on-prem)
- **Tabellenformat**: Apache Iceberg auf allen Schichten
- **Catalog**: Lakekeeper (REST)

Master-Architektur-Dokument: [`./lakehouse-architektur.md`](./lakehouse-architektur.md)
— enthält die Begründungen aller Top-Level-Entscheidungen.

---

## 2. Verbindliche Architektur-Entscheidungen

| Entscheidung | Status | Quelle |
|---|---|---|
| Iceberg auf allen Schichten als Storage-Format | verbindlich | lakehouse-architektur.md |
| Lakekeeper als Single Catalog (alle Engines lesen daraus) | verbindlich | lakehouse-architektur.md |
| StarRocks als Serving-Engine, Modus **shared-data** | verbindlich (Alternativen ClickHouse/Trino verworfen) | lakehouse-architektur.md |
| Spark auf K8s für Batch + Iceberg-Maintenance | verbindlich | lakehouse-architektur.md |
| Flink für Streaming (Kafka → Iceberg) | verbindlich | lakehouse-architektur.md |
| Argo Workflows für Orchestrierung | verbindlich | lakehouse-architektur.md |
| MinIO als Storage-Backend (on-prem, S3-kompatibel) | verbindlich | lakehouse-architektur.md, Abschnitt "Storage-Entscheidung" |
| Transform-Spec-Runner: deklarative Batch-Transformationen als eigener Mini-Layer auf Spark (statt externem Tool wie dbt) | verbindlich (v1) | transform/ |

**Folgerung**: Alternativen zu den verbindlichen Punkten **nicht** ohne neuen
Trigger vorschlagen. Optimierungen innerhalb des Stacks sind willkommen.

**Hinweis Transform-Runner / Kafka**: Der Transform-Spec-Runner (`transform/`)
darf Kafka als Source nur als **bounded Batch-Read** (fester Offset-Bereich)
nutzen — kontinuierliches Streaming Kafka→Iceberg bleibt ausschließlich Flink.
Diese Abgrenzung ist verbindlich. Der Runner ist bewusst ein minimaler eigener
Layer; sobald der SQL-Anteil dominiert, ist dbt/SQLMesh der Migrations-Trigger.

---

## 3. Folder-Map

```
swimming-pool/
├── AGENTS.md                       <- dieses Dokument
├── lakehouse-architektur.md        <- Master-Plan
├── iceberg/                        <- Tabellen-Design + Maintenance
├── starrocks/                      <- Serving-Engine
│   └── mcp/                        <- StarRocks-MCP-Server (OpenCode-Integration)
├── spark/                          <- Compute-Engine (Batch + Maintenance)
├── lakekeeper/                     <- Catalog-Server (OpenFGA-Authz)
│   └── mcp/                        <- Lakekeeper-MCP-Server (OpenCode-Integration)
├── oidc/                           <- (leer, geplant)
└── transform/                      <- deklarative Batch-Transformationen (Spec-Runner)
```

Jeder Subfolder bekommt unten einen eigenen Abschnitt. Bei neuem Subfolder:
diesen hier erweitern und einen entsprechenden Abschnitt 4ff. anlegen.

---

## 4. ./iceberg/

### Zweck
Tabellen-Design-Konventionen + agnostische Maintenance-Pipeline.

### Inhalt
- [`./iceberg/README.md`](./iceberg/README.md) — Quick-Start, Verzeichnis-Übersicht
- [`./iceberg/table-design.md`](./iceberg/table-design.md) — Hidden Partitioning, Sort Order, File Sizes, Properties, Schema-Evolution, Anti-Patterns
- [`./iceberg/maintenance.md`](./iceberg/maintenance.md) — Job-Konzept (Compaction, Snapshot Expiry, Orphan Files, Manifest Rewrite, Position Deletes), Tool-Vergleich, Reihenfolge der Jobs
- [`./iceberg/spark/maintenance.py`](./iceberg/spark/maintenance.py) — PySpark-Job, Catalog-Discovery, liest `maintenance.*.*` aus TBLPROPERTIES
- [`./iceberg/argo/maintenance-workflow.yaml`](./iceberg/argo/maintenance-workflow.yaml) — WorkflowTemplate + 2 CronWorkflows (daily + weekly)

### Verbindliche Konventionen
- **Format-Version 2** für alle Tabellen
- **Hidden Partitioning** (z.B. `days(event_ts)`), keine explizite Partition-Spalte
- **Compression**: `zstd` Level 3
- **Target File Size**: 512 MB; Row Group: 128 MB
- **Naming als Lakekeeper-Namespaces**: `bronze.<source>` / `silver.<domain>` / `gold.<domain>` / `serving.<consumer>` / `mart.<consumer>`
- **`metrics.default = truncate(16)`** global; `full` selektiv für Filter-Hot-Spalten (Numerics/Dates immer `full`); `none` für Memo/JSON/Binary
- **Sort Order** via `ALTER TABLE ... WRITE ORDERED BY (...)`, NICHT über Properties (kein `write.sort-order`)
- **Maintenance-Steuerung pro Tabelle** ausschließlich über TBLPROPERTIES `maintenance.*.*` (Opt-out pro Tabelle, sonst Defaults)
- **Reihenfolge der Maintenance-Jobs**:
  1. `rewrite_data_files` (Compaction)
  2. `rewrite_position_delete_files` (nur Upsert/CDC-Tabellen)
  3. `expire_snapshots`
  4. `rewrite_manifests`
  5. (separat, wöchentlich) `remove_orphan_files`

### Wichtige Faktoren-Korrekturen
- **`rewrite_data_files`-Default ist `binpack`** — `strategy='sort'` muss explizit angegeben werden
- **`strategy='sort'` ohne Sort-Order = Fehler.** Vorher prüfen via `SHOW CREATE TABLE` (Mensch) oder py4j (`Spark3Util.loadIcebergTable(...).sortOrder().isUnsorted()`) (Maschine)
- **Property-Änderungen wirken nur write-side.** Existierende Files brauchen `rewrite_data_files` zum Retrofit
- **`rewrite_manifests` rechnet Stats NICHT neu** — nur Konsolidierung
- **`remove_orphan_files`** ist destruktiv: niemals parallel zu Writes, `older_than` mind. 3 Tage

### Bei Änderungen achten auf
- TBLPROPERTIES-Schlüssel zwischen `maintenance.md` und `spark/maintenance.py` synchron halten
- Beispiel-DDLs in `table-design.md` und Defaults in `maintenance.py` synchron halten

### Offene Punkte
- Compaction-WHERE-Filter in `maintenance.py` ist Platzhalter (`_PARTITION_TIME`); soll dynamisch aus Partition-Spec gebaut werden
- Sort-Order-Detection im Job ist beschrieben, aber noch nicht im Code ergänzt
- Lakekeeper-Auth (OIDC) im Spark-Catalog-Config noch unauthenticated

---

## 5. ./starrocks/

### Zweck
Serving-Engine-Deployment auf K8s + SQL-Patterns + Workload-Isolation +
Materialisierungs-Strategie.

### Inhalt (Top-Level)
- [`./starrocks/README.md`](./starrocks/README.md) — Übersicht, Deployment-Reihenfolge, Validation-Checkliste, Autoscaling-Voraussetzungen
- [`./starrocks/prometheus.md`](./starrocks/prometheus.md) — Metrics-Endpoints, Operator- vs Standalone-Pfad, PromQL-Queries
- [`./starrocks/values-prod.yaml`](./starrocks/values-prod.yaml) — Helm-Values Produktion (kube-starrocks 1.11.4)
- [`./starrocks/values-dev.yaml`](./starrocks/values-dev.yaml) — Dev-Variante
- [`./starrocks/configmap-fe.yaml`](./starrocks/configmap-fe.yaml) / [`./starrocks/configmap-cn.yaml`](./starrocks/configmap-cn.yaml) — Custom Tunings

### Inhalt (./starrocks/sql/)
- `lakekeeper-catalog.sql` — External Catalog Setup
- `resource-groups.sql` — Workload-Isolation (rg_api/rg_bi/rg_etl)
- `example-mv.sql` — Minimal-MV
- `mv-patterns.sql` — drei Patterns: Tages-Aggregat, Hot-Subset, Hot-Cache
- `query-optimization.md` — Cheatsheet (Layout, Indizes, Stats, JOIN-Tuning, Anti-Patterns)

### Inhalt (./starrocks/argo/, ./starrocks/secrets/)
- `argo/mv-orchestration.yaml` — WorkflowTemplate + CronWorkflow + DAG für MV-Refresh
- `secrets/starrocks-s3-credentials.example.yaml` — Secret-Template (NIE mit echten Werten committen)

### Inhalt (./starrocks/mcp/)
- [`./starrocks/mcp/README.md`](./starrocks/mcp/README.md) — Offline-Setup des
  offiziellen StarRocks-MCP-Servers (`mcp-server-starrocks`) für OpenCode:
  Wheels → Nexus → uv → opencode.json. Gibt dem LLM SQL-Zugriff auf alle
  Iceberg-Schichten via `lake`-Catalog (inkl. `analyze_query`, `table_overview`).

### Verbindliche Konventionen
- **Helm-Chart**: `starrocks/kube-starrocks` Version `1.11.4` (verifiziert)
- **Modus**: `runMode: shared_data`
- **StarRocks-Version**: `3.3.5` gepinnt (mind. 3.3 wegen Multi-Warehouse + Iceberg-REST-Stabilität)
- **CN-Replicas: NICHT zusätzlich zu `autoScalingPolicy` setzen** — Operator nullt `replicas` bei aktivem HPA
- **HPA-Version**: `v2` (nicht `v2beta2` — entfernt seit K8s 1.26)
- **HPA `scaleDown.selectPolicy: Disabled`** (Cache-Erhalt geht vor Scaling)
- **`configMapInfo` ersetzt Default-Configs vollständig** — Standard-Ports, JAVA_OPTS, LOG_DIR müssen explizit in den ConfigMaps stehen
- **MV-Properties**: `storage_volume = builtin_storage_volume`, `replication_num = 1`
- **MV-DDL gehört in den Argo-Workflow** der Mart-Pipeline; nicht ad hoc in StarRocks erzeugen
- **Resource Groups**: `rg_api` / `rg_bi` / `rg_etl` als Standard-Trennung; in StarRocks 3.x heißt das Feld **`cpu_weight`**, nicht `cpu_core_limit`
- **Big-Query-Limits** (`big_query_cpu_second_limit`, `big_query_scan_rows_limit`, `big_query_mem_limit`) für `rg_api` setzen, gegen Runaway-Queries

### Cross-Catalog-Caveat (kritisch)
**StarRocks-MVs sind NICHT in Lakekeeper sichtbar.** Sie liegen in StarRocks'
`default_catalog` mit eigenem Segment-Format, nicht als Iceberg-Tabellen.
Wenn Aggregate auch in Lakekeeper sichtbar sein sollen → Spark-Aggregation
oder StarRocks `INSERT INTO` schreiben Iceberg-Marts in den `lake`-Catalog
(Master-Plan Option C); StarRocks-MVs sitzen optional als Speed-Layer obendrauf.

### Multi-Warehouse (optional, deaktiviert)
- Realisiert über separate **`StarRocksWarehouse`-CRDs** (in Operator 1.11.4 vorhanden), NICHT via Helm-Values
- Beispiel-Manifest im Kommentar von `values-prod.yaml`
- Trigger-Punkte für Aktivierung: API-Cache-Pollution durch BI, harte Tenant-Isolation, separate Skalierung pro Workload
- Resource Groups bleiben **zusätzlich** aktiv (Quota innerhalb eines Warehouse)

### Bei Änderungen achten auf
- Property-Namen je StarRocks-Version (`cpu_core_limit` vs `cpu_weight`)
- Helm-Chart-Felder können je Version unterschiedlich sein — bei Konflikt gegen `https://raw.githubusercontent.com/StarRocks/starrocks-kubernetes-operator/v1.11.4/helm-charts/charts/kube-starrocks/values.yaml` verifizieren

### Offene Punkte
- Multi-Warehouse-CRDs noch nicht als konkrete Files angelegt
- OIDC-Anbindung an Lakekeeper-Catalog: TODO (siehe `./oidc/`)
- Auth-Mapping Lakekeeper-Permissions → StarRocks-Rollen: TODO
- Ingress für externes MySQL-Protokoll (Port 9030 + TLS): TODO
- `resource-groups.sql` enthält noch `cpu_core_limit` — sollte beim PoC gegen 3.3.5 verifiziert und ggf. auf `cpu_weight` migriert werden

---

## 6. ./spark/

### Zweck
Compute-Engine für Batch-ETL und Iceberg-Maintenance. Auf K8s via Kubeflow
Spark Operator (`sparkoperator.k8s.io/v1beta2`).

### Inhalt
- [`./spark/README.md`](./spark/README.md) — Index + bestehende Artefakte + offene Punkte
- [`./spark/architecture.md`](./spark/architecture.md) — Driver/Executor/Tasks, Catalyst, Tungsten, AQE, Shuffles, Joins, Caching
- [`./spark/kubernetes.md`](./spark/kubernetes.md) — Spark-Operator, `SparkApplication`-CRD, Pod-Modell, RBAC, Code-Distribution (4 Patterns inkl. venv-pack), Logs, History Server
- [`./spark/best-practices.md`](./spark/best-practices.md) — Sizing, Iceberg-Configs, Schreib-Patterns, Anti-Patterns, Debug-Workflow, Reproduzierbarkeit
- [`./spark/incremental-processing.md`](./spark/incremental-processing.md) — `Trigger.AvailableNow` + Streaming-Checkpoint für inkrementelle Cron-Jobs, mit Verifikations-Checks

### Etablierte Patterns (aus dem bestehenden Repo)
- **Spark-Version `3.5.1`** (gepinnt in `iceberg/argo/maintenance-workflow.yaml`)
- **Iceberg-Spark-Runtime `1.6.0`** (gleicher Ort)
- **Catalog-Name `lake`** (vereinheitlicht über alle Engines)
- **Code-Distribution**: stable Runner-Image + Anwendungs-Code als **venv-pack-Archiv** (job-spezifische Python-Dependencies); ConfigMap-Mount nur für kleine Skripte wie `iceberg/spark/maintenance.py`
- **Submission via Argo-WorkflowTemplate**, das `SparkApplication`-CRDs deklarativ erzeugt und auf Status `COMPLETED` wartet
- **Restart-Policy**: `OnFailure`, max 2 Retries (siehe `iceberg/argo/maintenance-workflow.yaml`)

### Wichtige Klarstellungen
- **`df.checkpoint()` ≠ Streaming-Checkpoint.** Erstes ist Lineage-Truncation **innerhalb** eines Runs (für ML/Graph-Workloads), zweites ist Source-Position+State **über Runs hinweg**. Im Lakehouse-Kontext fast nur das Streaming-Variante relevant.
- **Resume-zwischen-Runs gibt es für reines Batch nicht** — DataFrame hat kein "Fortschritt"-Konzept. Wer Resume will, formuliert das Problem als Streaming mit `Trigger.AvailableNow` (siehe `spark/incremental-processing.md`).
- **Iceberg-Streaming-Sinks**: `.toTable("lake.gold.x")`, NICHT `.option("path", ...).start()`. Letzteres ist ambig (Pfad vs. Tabellen-Identifier).
- **`Trigger.AvailableNow` ist kein Dauerbetrieb** — Job startet, arbeitet alles aktuell Verfügbare ab, exit. Pro Run **eine** Sink-Commit (bei richtiger Config), strukturell wie Pure Batch.

### Bei Änderungen achten auf
- `spark/incremental-processing.md` und `iceberg/maintenance.md` — Streaming-Checkpoint-Bucket muss aus `remove_orphan_files` ausgenommen werden
- Operator-Risiken (Mutating Webhook + `failurePolicy`, HA-Mode, namespace-Scope) sind in der Konversation diskutiert, aber **noch nicht** in `spark/kubernetes.md` festgehalten — Lücke

### Offene Punkte
- Eigenes Runner-Image mit gepinnten Iceberg-/AWS-Bundle-JARs noch nicht gebaut — Maintenance-Workflow nutzt aktuell `--packages` (langsamer Start, brüchig)
- Spark History Server noch nicht deployed
- Webhook-/Operator-Betriebs-Abschnitt fehlt in `spark/kubernetes.md`
- OIDC gegen Lakekeeper aus Spark heraus noch unauthenticated
- Pattern für Streamende Iceberg-Writes via Spark Structured Streaming für Continuous-Cases (jenseits AvailableNow) noch nicht behandelt

---

## 7. ./lakekeeper/

### Status
Helm-Deployment + OpenFGA-Authorizer + Role-Sync-Sidecar vorhanden; MCP-Server
für OpenCode ergänzt. [`./lakekeeper/README.md`](./lakekeeper/README.md) ist die
Single Source of Truth für das Deployment.

### Inhalt (Top-Level)
- [`./lakekeeper/README.md`](./lakekeeper/README.md) — Deployment-Reihenfolge, OpenFGA-Setup, Role-Sync-Sidecar
- `openfga/` — Helm-Values für den OpenFGA-Authorizer-Dienst
- `role-sync/` — Keycloak→Lakekeeper Role-Sync (`sync.py` + ConfigMap)
- `cedar/` — inaktiv; Referenz für eine spätere Lakekeeper+-Evaluierung
- `values-dev.yaml` / `values-prod.yaml` — Lakekeeper-Helm-Overrides

### Inhalt (./lakekeeper/mcp/)
- [`./lakekeeper/mcp/server.py`](./lakekeeper/mcp/server.py) — eigener MCP-Server:
  liefert OpenCode inhaltliche Catalog-Metadaten (Namespaces, Tabellen-Schema +
  Kommentare, Partition-Spec, Snapshots) — kein Catalog-Management
- [`./lakekeeper/mcp/README.md`](./lakekeeper/mcp/README.md) — Offline-Setup
  (Wheels → Nexus → uv → opencode.json) + Code-Aufbau zum eigenständigen Erweitern
- [`./lakekeeper/mcp/opencode-commands.md`](./lakekeeper/mcp/opencode-commands.md) —
  Kurz-Notiz: wiederkehrende Analyse-Abläufe als OpenCode-Slash-Command, mit Beispiel

### Verbindliche Konventionen
- MCP-Server ist **inhaltliche Discovery, kein Management** — die Tools bleiben
  read-only (`list_namespaces`, `list_tables`, `describe_table`, `list_snapshots`)
- MCP-Auth: eigener Keycloak-Client (`svc-opencode-mcp`), **nicht** der
  Role-Sync-Client `svc-lakekeeper-sync`

### Geplanter Inhalt
- Backup-Policy für Lakekeeper-DB (Postgres)

### Bekannte Constraints
- Lakekeeper macht **kein** automatisches Iceberg-Maintenance — siehe [`./iceberg/maintenance.md`](./iceberg/maintenance.md)

---

## 8. ./oidc/

### Status
Leer. Konzept folgt.

### Geplanter Inhalt
- IdP-Konfiguration für Lakekeeper, StarRocks, Spark, Argo
- Token-Lebenszyklen
- Service-Account-Konzept (`svc_api`, `svc_etl`, `svc_bench`, ...)

---

## 9. ./transform/

### Zweck
Deklaratives Transformations-Framework: config-getriebener Spec-Runner für
häufige Batch-Verarbeitungen (Deduplizierung, Spalten-Mapping, Aggregation,
Filter, Cast) Bronze→Silver→Gold. Kein pipeline-spezifischer Code — die
Verarbeitung steht als YAML-Spec. Mirror des `iceberg/`-Maintenance-Musters
(generischer Job + generisches WorkflowTemplate + Registry).

### Inhalt
- [`./transform/README.md`](./transform/README.md) — Quick-Start, Deployment, Code-Distribution
- [`./transform/transform-spec.md`](./transform/transform-spec.md) — Spec-Schema-Referenz (source/schema/steps/sink) + Verifikations-Checkliste
- [`./transform/spark/runner.py`](./transform/spark/runner.py) — generischer PySpark-Job, Registries `SOURCES`/`STEPS`/`SINKS`
- [`./transform/argo/transform-workflow.yaml`](./transform/argo/transform-workflow.yaml) — WorkflowTemplate `spark-transform` + Beispiel-CronWorkflow + DAG
- [`./transform/pipelines/`](./transform/pipelines/) — versionierte Pipeline-Specs

### Verbindliche Konventionen
- **Ein generischer Runner + ein WorkflowTemplate** — fachliche Logik gehört
  ausschließlich in die Spec, nie in `runner.py` hardcoden
- **Source/Step/Sink über Registries erweitern** (`SOURCES`/`STEPS`/`SINKS`) —
  neuer Typ = Handler + Registry-Eintrag, kein Eingriff in `main()`
- **v1-Sources**: `iceberg`, `file` (csv/json/parquet/orc, auch komprimiert),
  `kafka`; **v1-Sink**: `iceberg`
- **Kafka-Source nur bounded Batch-Read** (fester Offset-Bereich), nie
  continuous — Streaming Kafka→Iceberg bleibt Flink
- **Sink-Modes**: `append` / `overwrite_partitions` / `merge` — Pattern D
  (`insertInto`) ist nicht erlaubt
- **Sink-Tabellen** werden per DDL vorab/menschlich versioniert angelegt
  (kuratierte TBLPROPERTIES, siehe `table-design.md`); `create_if_not_exists`
  ist Notnagel, nicht der Normalfall
- **Specs versioniert in `pipelines/`** — je Spec ein fachlich geowntes
  Artefakt; Cron/DAG dazu im `transform-workflow.yaml`

### Bezug zu anderen Foldern
- Compute- und Schreib-Patterns (A/B/C): `./spark/best-practices.md`
- Sink-Tabellen-Design + Maintenance-Properties: `./iceberg/`
- `merge`-Sink erzeugt Position-Deletes → Sink-Tabelle braucht
  `maintenance.position_deletes.enabled=true` (siehe `iceberg/maintenance.md`)

### Offene Punkte
- Inkrementelle Verarbeitung (`source.incremental`) — v1 ist Full-Refresh-Batch
- `value_format: avro` für Kafka-Sources noch nicht implementiert
- Sink-seitige Schema-Validierung gegen die Ziel-Tabelle
- venv-pack-Distribution statt ConfigMap-Mount (ConfigMap-1-MiB-Limit)
- `PyYAML` muss im Spark-Image vorhanden sein (PoC-Caveat)

---

## 10. Cross-cutting-Konventionen

### Naming
- Iceberg-Namespaces: `bronze.<source>` / `silver.<domain>` / `gold.<domain>` / `serving.<consumer>` / `mart.<consumer>`
- StarRocks External Catalog: `lake` (singular, vereinheitlicht)
- Argo-Workflows: `<engine>-<job>-<frequency>` (z.B. `iceberg-daily-maintenance`)
- Service-User: `svc_api`, `svc_etl` (Singular Service); `bi_team` (Plural Gruppe)
- Resource Groups: `rg_<workload>`
- Warehouses (falls aktiviert): `wh_<workload>`

### Governance
- DDL/Schema-Changes über die jeweilige Schreib-Pipeline (Spark/Flink), nicht ad hoc per SQL
- MV-Definitionen werden zusammen mit ihrer Mart-Pipeline versioniert
- Transformations-Pipelines werden als versionierte Spec-Dateien in `transform/pipelines/` geownt — analog zu MV-Definitionen
- Maintenance ist **agnostisch** — eine Pipeline pro Engine, alle Tabellen via Catalog-Discovery + Properties

### Agnostik-Prinzip
Tabellenspezifische Konfiguration → über Properties/TBLPROPERTIES, nicht
als Listen in YAML-Files. Jobs lesen Properties zur Laufzeit. Neue Tabelle
anlegen = automatisch erfasst.

**Klarstellung Transform-Specs**: Eine Per-Pipeline-YAML in
`transform/pipelines/` verletzt dieses Prinzip **nicht**. Das Prinzip richtet
sich gegen zentrale Tabellen-*Listen* für Querschnitts-Operationen (z.B.
Maintenance). Eine Transformation ist dagegen ein diskretes, fachlich
geowntes Artefakt, dessen Logik sich nicht aus Catalog-Properties ableiten
lässt — genau wie eine MV-Definition. Agnostisch bleibt der *Runner*: eine
Code-Basis, ein WorkflowTemplate; eine neue Pipeline = nur eine neue Spec-Datei.

### Verifikation gegen externe Quellen
Versionen, die im Repo gepinnt sind:

| Komponente | Version | Quelle |
|---|---|---|
| StarRocks | `3.3.5` | values-prod.yaml |
| kube-starrocks Helm Chart | `1.11.4` | (extern, Helm-Repo) |
| Apache Spark | `3.5.1` | iceberg/argo/maintenance-workflow.yaml |
| iceberg-spark-runtime | `1.6.0` | iceberg/argo/maintenance-workflow.yaml |

Bei Property-/Feld-Unsicherheit: gegen die offiziellen Repos verifizieren
statt aus dem Gedächtnis raten. Insbesondere:
- StarRocks-Operator: github.com/StarRocks/starrocks-kubernetes-operator (Tag-Version!)
- StarRocks SQL: docs.starrocks.io
- Iceberg: iceberg.apache.org/docs/

---

## 11. Anti-Patterns (was NICHT zu tun ist)

| Anti-Pattern | Warum |
|---|---|
| StarRocks-MVs für "Aggregate, die in Lakekeeper sichtbar sein sollen" | MVs sind StarRocks-internal, nicht Iceberg → unsichtbar in Lakekeeper |
| `replicas` zusätzlich zu `autoScalingPolicy` setzen (CN) | Operator nullt `replicas` bei HPA → Konflikt/Verwirrung |
| `metrics.default = full` für alle Iceberg-Spalten | Manifest-Bloat, Plan-Step lahm; nur selektiv setzen |
| `strategy='sort'` ohne Sort-Order-Check | Crashed bei Tabellen ohne `WRITE ORDERED BY` |
| `remove_orphan_files` parallel zu Writes | Kann in-flight Writes löschen |
| Per-Tabelle Maintenance-Cronjob anlegen | Bricht Agnostik — TBLPROPERTIES nutzen |
| Eigene Catalogs pro Engine | Lakekeeper ist Single Source of Truth |
| `cpu_core_limit` in StarRocks 3.x Resource Groups | In 3.x heißt das Feld `cpu_weight` |
| `version: v2beta2` für HPA in K8s ≥ 1.26 | API entfernt → `version: v2` |
| Default `serviceMonitor.enabled: true` ohne kube-prometheus-stack | ServiceMonitor wird ignoriert; bei Standalone-Prometheus scrape_config nötig |
| `df.checkpoint()` für "Resume zwischen Cron-Runs" verwenden | Ist nur Lineage-Truncation **innerhalb** einer App; Cross-Run-Resume = Streaming-Checkpoint mit `Trigger.AvailableNow` |
| `.option("path", "<table>")` + `.start()` für Streaming-Iceberg-Sinks | Ambig — Pfad oder Tabelle? Sauber ist `.toTable("lake.<ns>.<table>")` (geht durch den Catalog) |
| Streaming-Checkpoint-Bucket im Iceberg-`remove_orphan_files` | Cleanup löscht Checkpoint-Files → Stream beim nächsten Start kaputt; Bucket organisatorisch trennen |
| Custom Image als alleinige Code-Distribution-Strategie pushen | Wir nutzen Runner-Image + venv-pack, NICHT Code im Image |
| Pushgateway für StarRocks- oder Spark-Service-Metriken | Pushgateway = Batch-Jobs; für Services scrape direkt (siehe `starrocks/prometheus.md`) |
| Pattern D (`insertInto` / dynamic partition overwrite) im Transform-Runner-Sink | Geht an der Iceberg-`writeTo`-API vorbei; `overwrite_partitions` (Pattern B) nutzen |
| Pipeline-Logik in `transform/spark/runner.py` hardcoden statt in die Spec | Bricht das config-getriebene Prinzip — der Runner bleibt generisch, Fachliches gehört in die YAML-Spec |

---

## 12. Wenn dieses Dokument geändert wird

| Änderung | Was anpassen |
|---|---|
| Neue Architektur-Entscheidung | Abschnitt 2 |
| Neuer Subfolder | Abschnitt 3 + neuen Abschnitt 4ff. |
| Neue Konvention pro Engine | passender Abschnitt 4–9 |
| Neues Anti-Pattern festgestellt | Abschnitt 11 |
| Versions-Pinning geändert | Abschnitt 10 |
| Master-Plan-Update (`lakehouse-architektur.md`) | Abschnitt 1 oder 2 ggf. nachziehen |

Bei Unklarheit über die Aktualität eines Punkts: gegen die referenzierten
Files prüfen — diese sind die Single Source of Truth, dieses Dokument ist
ein Index/Summary darüber.
