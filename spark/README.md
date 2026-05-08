# Spark — Compute-Engine für Batch und Maintenance

Apache Spark auf Kubernetes via **Spark Operator**. Verwendet für:

- Batch-ETL Bronze → Silver → Gold → Mart (Iceberg-Writes)
- Iceberg-Maintenance (Compaction, Snapshot-Expiry, Orphan-Cleanup) —
  konkret implementiert in [`../iceberg/`](../iceberg/)
- ML / Notebooks gegen den Lakehouse (perspektivisch)
- Cross-Catalog-Aggregate (Iceberg-Marts schreiben, falls
  StarRocks-MV-Pattern nicht reicht — siehe Master-Plan Option C)

## Inhalt dieses Folders

| Datei | Inhalt |
|---|---|
| [`architecture.md`](./architecture.md) | Wie Spark intern arbeitet: Driver/Executor/Tasks, Catalyst, Tungsten, AQE, Shuffles, Joins, Caching. Konzeptionell, ohne K8s. |
| [`kubernetes.md`](./kubernetes.md) | Spark-Operator, `SparkApplication`-CRD, Pod-Modell, RBAC, Dependencies, Logs, History Server, Networking, Lebenszyklus. |
| [`best-practices.md`](./best-practices.md) | Sizing-Empfehlungen, Iceberg-Configs, Schreib-Patterns, Anti-Patterns, Debug-Workflow, lokales Testen, Reproduzierbarkeit. |
| [`incremental-processing.md`](./incremental-processing.md) | Wiederaufsetz-Sicherheit für cron-getriggerte Jobs: `Trigger.AvailableNow` + Streaming-Checkpoint, Idempotent-Overwrite-Variante, Small-File-Vermeidung. |

Empfohlene Lese-Reihenfolge:
1. `architecture.md` — Grundlagen verstehen
2. `kubernetes.md` — Wie das im Cluster konkret läuft
3. `best-practices.md` — Was im Tagesgeschäft beachten

## Bestehende Spark-Artefakte im Repo

- [`../iceberg/spark/maintenance.py`](../iceberg/spark/maintenance.py) — agnostischer
  Maintenance-Job, Beispiel für Catalog-Discovery + TBLPROPERTIES-getriebene Logik
- [`../iceberg/argo/maintenance-workflow.yaml`](../iceberg/argo/maintenance-workflow.yaml) —
  Argo-WorkflowTemplate, die `SparkApplication`-CRDs deklarativ erzeugt und auf
  Status `COMPLETED` wartet

## Offene Punkte

- Auth (OIDC) gegen Lakekeeper aus Spark heraus ist noch nicht konfiguriert
  — siehe [`../oidc/`](../oidc/) sobald dort konkretisiert
- Pattern für streamende Iceberg-Writes (Spark Structured Streaming als
  Alternative zu Flink für einfachere Cases) noch nicht behandelt
