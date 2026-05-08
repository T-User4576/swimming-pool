# AGENTS.md — Lakehouse-Projekt

Dieses Dokument gibt Agents (auch leichteren Modellen) den Kontext, um im
Repository sinnvolle Vorschläge zu machen, ohne die Architektur-Geschichte
des Projekts neu ableiten zu müssen. Lies diesen Abschnitt vor jedem Task.

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

**Folgerung**: Alternativen zu den verbindlichen Punkten **nicht** ohne neuen
Trigger vorschlagen. Optimierungen innerhalb des Stacks sind willkommen.

---

## 3. Folder-Map

```
swimming-pool/
├── AGENTS.md                       <- dieses Dokument
├── lakehouse-architektur.md        <- Master-Plan
├── iceberg/                        <- Tabellen-Design + Maintenance
├── starrocks/                      <- Serving-Engine
├── spark/                          <- Compute-Engine (Batch + Maintenance)
├── lakekeeper/                     <- (leer, geplant)
└── oidc/                           <- (leer, geplant)
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
Leer. Konzept folgt.

### Geplanter Inhalt
- Helm-Deployment des Lakekeeper-Servers
- OIDC-Integration (mit `./oidc/`)
- Permissions-Modell pro Namespace (`bronze.*`, `silver.*`, `gold.*`, `serving.*`, `mart.*`)
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

## 9. Cross-cutting-Konventionen

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
- Maintenance ist **agnostisch** — eine Pipeline pro Engine, alle Tabellen via Catalog-Discovery + Properties

### Agnostik-Prinzip
Tabellenspezifische Konfiguration → über Properties/TBLPROPERTIES, nicht
als Listen in YAML-Files. Jobs lesen Properties zur Laufzeit. Neue Tabelle
anlegen = automatisch erfasst.

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

## 10. Anti-Patterns (was NICHT zu tun ist)

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

---

## 11. Wenn dieses Dokument geändert wird

| Änderung | Was anpassen |
|---|---|
| Neue Architektur-Entscheidung | Abschnitt 2 |
| Neuer Subfolder | Abschnitt 3 + neuen Abschnitt 4ff. |
| Neue Konvention pro Engine | passender Abschnitt 4–8 |
| Neues Anti-Pattern festgestellt | Abschnitt 10 |
| Versions-Pinning geändert | Abschnitt 9 |
| Master-Plan-Update (`lakehouse-architektur.md`) | Abschnitt 1 oder 2 ggf. nachziehen |

Bei Unklarheit über die Aktualität eines Punkts: gegen die referenzierten
Files prüfen — diese sind die Single Source of Truth, dieses Dokument ist
ein Index/Summary darüber.
