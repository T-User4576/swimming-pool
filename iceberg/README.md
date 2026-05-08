# Iceberg — Tabellen-Design & Maintenance

Die Datenformat-Konventionen + Operations-Bausteine für das Lakehouse.
Catalog ist Lakekeeper (siehe `../lakekeeper/`), Storage MinIO/S3.

Übergeordneter Architektur-Plan: [`../lakehouse-architektur.md`](../lakehouse-architektur.md)

## Verzeichnis-Inhalt

```
iceberg/
├── README.md                              # diese Datei
├── table-design.md                        # Partitionierung, Sort, File-Sizes,
│                                          #   Properties, Schema-Evolution.
│                                          #   Fokus: auch kalte Reads performant.
├── maintenance.md                         # Welche Jobs, wie oft, mit welchem Tool.
│                                          #   Inkl. Tool-Vergleich + Konvention
│                                          #   für agnostischen Betrieb.
├── spark/
│   └── maintenance.py                     # PySpark-Job: Catalog-Discovery,
│                                          #   liest TBLPROPERTIES, ruft Procedures.
└── argo/
    └── maintenance-workflow.yaml          # WorkflowTemplate + zwei CronWorkflows
                                           #   (daily + weekly) -- agnostisch.
```

## Kernidee: agnostische Maintenance

Niemand will pro neuer Tabelle einen Cron-Job anpassen. Daher:

1. **Lakekeeper / Iceberg Catalog ist Source of Truth** — der Spark-Job listet
   alle Tabellen via `SHOW NAMESPACES` / `SHOW TABLES`.
2. **TBLPROPERTIES steuern Verhalten pro Tabelle** — Konvention `maintenance.*.*`
   (Details in `maintenance.md` Abschnitt 9).
3. **Globale Defaults im PySpark-Job** — gilt, wenn Property nicht gesetzt.
4. **Opt-out per Property** — `maintenance.compaction.enabled = false`.

Neue Tabelle anlegen → wird beim nächsten Cron automatisch maintained.
Spezial-Behandlung nötig → eine `ALTER TABLE ... SET TBLPROPERTIES`-Zeile.

## Quick-Start

1. **`iceberg/spark/maintenance.py`** als ConfigMap deployen:
   ```bash
   kubectl create configmap iceberg-maintenance-script \
     --from-file=maintenance.py=iceberg/spark/maintenance.py \
     -n spark
   ```

2. **Argo-Workflows applien**:
   ```bash
   kubectl apply -f iceberg/argo/maintenance-workflow.yaml
   ```

3. **Erster Lauf manuell**, mit `--dry-run`:
   ```bash
   argo submit --from cronwf/iceberg-daily-maintenance -n argo \
     -p dry-run=true -p namespaces=gold
   ```

4. Wenn Logs sauber → Cron läuft täglich 02:00 Uhr.

## Was du zusätzlich beachten solltest

- **Spark-Image**: `apache/spark:3.5.1-python3` ist Platzhalter. Wenn ihr ein
  eigenes Image mit gepinnten Iceberg-/AWS-Bundle-Versionen baut, das hier eintragen.
- **Iceberg-Version**: Workflow verwendet `iceberg-spark-runtime 1.6.0`. Muss zur
  Lakekeeper-Catalog-API-Version passen.
- **Auth zu Lakekeeper**: aktuell unauthenticated REST. Sobald OIDC angeschlossen
  ist (`../oidc/`), müssen die Spark-Configs `spark.sql.catalog.lake.token=...`
  o.ä. ergänzt werden.
- **Resource-Sizing**: die Defaults (5 Executors × 16 GB) sind ein Start für
  10–100 GB Tabellen-Bewegung. Bei TB pro Tabelle hochskalieren oder per
  Argo-Parameter überschreiben.

## Beobachtung im Betrieb

- Spark History Server für Job-Inspektion
- Argo-UI für Workflow-Status
- Iceberg-Selbstdiagnose-Queries (siehe `maintenance.md` Abschnitt 10):
  ```sql
  SELECT count(*), avg(file_size_in_bytes)/1024/1024 AS avg_mb
  FROM lake.gold.orders.files;
  ```
- Wenn `avg_mb < 64` → Compaction nicht effektiv, Job-Logs prüfen.
- Wenn `snapshot_count > 100` → Snapshot Expiry läuft nicht oder retain-window
  zu groß.
