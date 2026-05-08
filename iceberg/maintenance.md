# Iceberg — Maintenance-Konzept

Lakekeeper macht **kein** automatisches Maintenance. Bei mehreren TB/Tag wird das
Lakehouse ohne Maintenance innerhalb von Wochen unbenutzbar:

- Flink-Streaming-Writes erzeugen massenweise kleine Files → Plan-Step explodiert
- Snapshots akkumulieren → Plan-Step explodiert² + Storage-Kosten
- Failed Writes hinterlassen Orphan-Files → Storage-Kosten
- Manifests wachsen → Plan-Step lahm

## 1. Die Jobs im Überblick

| Job | Was | Frequenz | Kritikalität | Spark-Procedure |
|---|---|---|---|---|
| **Compaction** | Kleine Files → Target-Size; Sort-Order anwenden | täglich (Flink-Tabellen: stündlich) | hoch | `rewrite_data_files` |
| **Snapshot Expiry** | Alte Snapshots löschen | täglich | hoch (Storage) | `expire_snapshots` |
| **Orphan File Removal** | Verwaiste Daten-Files löschen | wöchentlich | mittel (Storage) | `remove_orphan_files` |
| **Manifest Rewrite** | Manifests konsolidieren | wöchentlich | mittel (Plan-Speed) | `rewrite_manifests` |
| **Position Delete Compaction** | Delete-Files in Daten konsolidieren | täglich (nur bei Upsert/CDC) | hoch | `rewrite_position_delete_files` |

## 2. Compaction (`rewrite_data_files`)

**Was**: Liest viele kleine Files, schreibt wenige große Files. Optional in Sort-Order.

**Default-Strategie ist `binpack`** — wenn du `strategy` weglässt, werden Files
nur konsolidiert, **nicht** sortiert. Für File-Skipping via Min/Max musst du
explizit `strategy => 'sort'` setzen.

```sql
CALL system.rewrite_data_files(
  table => 'lake.gold.orders',
  strategy => 'sort',                          -- ohne diese Zeile: binpack
  -- sort_order optional: ohne Param wird die in der Tabelle hinterlegte
  -- WRITE ORDERED BY verwendet (siehe table-design.md)
  options => map(
    'target-file-size-bytes', '536870912',
    'min-input-files', '5',
    'max-concurrent-file-group-rewrites', '5',
    'partial-progress.enabled', 'true',
    'rewrite-job-order', 'bytes-desc'   -- größte zuerst
  )
);
```

| Option | Default | Empfehlung |
|---|---|---|
| `strategy` | `binpack` | `sort` für Tabellen mit `WRITE ORDERED BY`, sonst `binpack` |
| `sort_order` | Table-Default (aus `WRITE ORDERED BY`) | leer lassen, dann nimmt Iceberg den Tabellen-Wert |
| `min-input-files` | 5 | bei 5 belassen — sonst lohnt der Rewrite nicht |
| `partial-progress.enabled` | `false` | auf `true` — bei Fehlern nicht alles verlieren |
| `rewrite-job-order` | `none` | `bytes-desc` — größte Probleme zuerst |

⚠️ **Wichtig**: `strategy='sort'` auf einer Tabelle **ohne** `WRITE ORDERED BY`
wirft einen Fehler ("table has no sort order"). Der Maintenance-Job muss das
vorher prüfen — siehe Abschnitt unten.

Bei sehr großen Tabellen: filtern auf "neue" Partitionen, sonst läuft der Job stundenlang:
```sql
CALL system.rewrite_data_files(
  table => 'lake.gold.orders',
  where => 'order_ts >= current_date() - interval 7 days'
);
```

### Sort-Order einer Tabelle prüfen

Bevor der Maintenance-Job `strategy='sort'` aufruft, muss er wissen, ob die
Tabelle überhaupt sortiert ist.

**SQL (am einfachsten, manuell)**:
```sql
SHOW CREATE TABLE lake.gold.orders;
-- Suche nach 'WRITE ORDERED BY (...)' Klausel im Output.
-- Fehlt sie → keine Sort-Order, strategy='sort' würde fehlschlagen.

DESCRIBE TABLE EXTENDED lake.gold.orders;
-- Ein Eintrag 'Sort Order' wird mit ausgegeben.
```

**PySpark (programmatisch im Maintenance-Job)**:
```python
def has_sort_order(spark, fqn: str) -> bool:
    iceberg_table = (
        spark._jvm.org.apache.iceberg.spark.Spark3Util
        .loadIcebergTable(spark._jsparkSession, fqn)
    )
    return not iceberg_table.sortOrder().isUnsorted()

strategy = "sort" if has_sort_order(spark, fqn) else "binpack"
```

Die Java-API über py4j ist die einzige saubere Variante in PySpark — Iceberg
exposed Sort-Order nicht über eine SQL-Metadata-Tabelle wie `<table>.partitions`.

## 3. Snapshot Expiry (`expire_snapshots`)

```sql
CALL system.expire_snapshots(
  table => 'lake.gold.orders',
  older_than => TIMESTAMP '2026-05-01 00:00:00',
  retain_last => 5,
  max_concurrent_deletes => 100
);
```

| Option | Empfehlung |
|---|---|
| `older_than` | `now() - 7 days` (Default eurer Pipeline) |
| `retain_last` | mind. 5 (für Time-Travel-Debugging) |
| `max_concurrent_deletes` | 50–200 — S3-API-Limits beachten |

**Achtung**: Wenn jemand Time-Travel-Queries (`AS OF TIMESTAMP ...`) nutzt, müssen die referenzierten Snapshots noch da sein. Default-Retention an längste benötigte Time-Travel-Distanz koppeln.

## 4. Orphan File Removal (`remove_orphan_files`)

**Was**: Findet Files in der Tabellen-Location, die kein aktueller oder gespeicherter Snapshot referenziert.

```sql
CALL system.remove_orphan_files(
  table => 'lake.gold.orders',
  older_than => TIMESTAMP '2026-04-30 00:00:00',
  max_concurrent_deletes => 50
);
```

⚠️ **Gefährlichster Maintenance-Job**: kann in-flight Writes löschen, wenn `older_than` zu kurz ist.

| Regel | Wert |
|---|---|
| `older_than` | mindestens **3 Tage** zurück, besser 7 |
| Concurrency mit Writes | nie parallel zu langen Spark-Writes |
| Pre-Flight | Erst `dry_run => true` (gibt nur Liste der Kandidaten) |

## 5. Manifest Rewrite (`rewrite_manifests`)

```sql
CALL system.rewrite_manifests('lake.gold.orders');
```

Wann nötig: wenn `SELECT count(*) FROM "lake"."gold"."orders.manifests"` → > 1000.

Billig auszuführen, wöchentlich Default.

## 6. Position Delete Compaction (CDC/Upsert-Tabellen)

Nur relevant wenn ihr `MERGE INTO` oder Flink-Upsert nutzt.

```sql
CALL system.rewrite_position_delete_files(
  table => 'lake.silver.orders_cdc',
  options => map('rewrite-all', 'true')
);
```

Wenn weggelassen: jede Read-Query muss zur Laufzeit Delete-Files mergen → langsam.

## 7. Tool-Vergleich

Wir wollen das nicht selbst stricken, wenn es etwas Brauchbares gibt:

| Tool | Lizenz | Fit für euer Setup | Anmerkungen |
|---|---|---|---|
| **Spark Iceberg Procedures** | Apache 2.0 | ✅ Ja, klar | Native, vollumfänglich, läuft auf eurem K8s-Spark. Kein Extra-Tool. |
| **Lakekeeper Maintenance** | Apache 2.0 | 🟡 Teilweise | Lakekeeper hat `expire_snapshots` als Hintergrundtask. Compaction noch nicht in Stable. |
| **Apache Polaris (Snowflake)** | Apache 2.0 | ❌ wäre Catalog-Wechsel | Hat eingebaute Maintenance, aber kein Lakekeeper-Drop-In. |
| **Tabular** | Commercial → seit 2024 Databricks | ❌ nicht nutzbar | Nicht mehr als unabhängiges Produkt verfügbar. |
| **Nimtable** | Apache 2.0 | 🟡 Beobachten | Visual UI + Maintenance-Trigger; jung, kleine Community. Eher als Companion-Tool. |
| **AWS Glue Iceberg Optimizer** | proprietär | ❌ AWS-only | Nur für Glue-managed Iceberg. |

**Empfehlung**: **Spark-Procedures via Argo CronWorkflows.**

Begründung:
- Ihr habt Spark auf K8s schon laufen
- Kein neues Tool, keine neue Auth, kein neues Deployment
- Volle Kontrolle über Schedule, Filter, Resource-Sizing
- Lakekeeper-eigene Maintenance kann später ergänzen, nicht ersetzen

## 8. Schedule-Empfehlung

| Job | Schedule | Resources |
|---|---|---|
| Compaction (Flink-Tabellen) | stündlich, gefiltert auf last 24h | 1× 8C/32G Driver, 5× 4C/16G Executors |
| Compaction (Batch-Tabellen) | täglich nachts, gefiltert auf last 7d | 1× 8C/32G, 10× 4C/16G |
| Snapshot Expiry | täglich nach Compaction | 1× 4C/16G, 5× 2C/8G |
| Orphan Files | wöchentlich Sonntag | 1× 8C/32G, 10× 4C/16G |
| Manifest Rewrite | wöchentlich nach Orphan | 1× 4C/16G |
| Position Delete Compaction | täglich (nur Upsert-Tabellen) | je nach Volumen |

## 9. Agnostisch betreiben — Konvention

Damit der Maintenance-Workflow nicht für jede neue Tabelle angepasst werden muss:

1. **Lakekeeper als Source of Truth**: Workflow listet via REST API alle Tabellen.
2. **TBLPROPERTIES als Config-Träger**: pro Tabelle steuern Properties Frequenz und Parameter.
3. **Globale Defaults**: gilt, wenn Property fehlt.

### Property-Konvention

| Property | Default | Wirkung |
|---|---|---|
| `maintenance.compaction.enabled` | `true` | Tabelle wird compacted |
| `maintenance.compaction.window_days` | `7` | Compaction-WHERE-Filter |
| `maintenance.compaction.target_file_size_bytes` | `536870912` | Ziel-File-Größe |
| `maintenance.snapshot.enabled` | `true` | Snapshot Expiry läuft |
| `maintenance.snapshot.retain_days` | `7` | Wie alte Snapshots behalten |
| `maintenance.snapshot.retain_last` | `5` | Mindestanzahl, auch wenn alt |
| `maintenance.orphan.enabled` | `true` | Orphan-Cleanup läuft |
| `maintenance.orphan.older_than_days` | `7` | Mindestalter für Orphan-Kandidaten |
| `maintenance.manifest.enabled` | `true` | Manifest-Rewrite |
| `maintenance.position_deletes.enabled` | `false` | Nur bei CDC/Upsert auf `true` |

Tabelle vom Maintenance ausschließen:
```sql
ALTER TABLE lake.bronze.heavy_archive SET TBLPROPERTIES (
  'maintenance.compaction.enabled' = 'false',
  'maintenance.snapshot.enabled' = 'false'
);
```

Konkrete Implementierung des Workflows: siehe `argo/maintenance-workflow.yaml`.

## 10. Observability — was monitoren

Spark Job Metrics in Prometheus exposed (via Spark Prometheus Sink) +
Lakekeeper-Metrics. KPIs:

| Metric | Quelle | Warnen bei |
|---|---|---|
| `iceberg_table_data_file_count` | Spark `system.files` | > 10.000 / Tabelle |
| `iceberg_table_avg_file_size_mb` | Spark | < 64 MB → Compaction nicht effektiv |
| `iceberg_table_snapshot_count` | Spark `system.snapshots` | > 100 |
| `iceberg_table_manifest_count` | Spark `system.manifests` | > 1000 |
| `maintenance_job_duration_seconds` | Argo + Spark | > 2× normaler Wert (Daten-Skew?) |
| `maintenance_job_failure` | Argo | > 0 |

Selbstdiagnose-SQL:
```sql
SELECT count(*) AS data_files,
       sum(file_size_in_bytes)/1024/1024/1024 AS gb_total,
       avg(file_size_in_bytes)/1024/1024 AS avg_mb
FROM lake.gold.orders.files;

SELECT count(*) AS snapshot_count,
       min(committed_at) AS oldest,
       max(committed_at) AS newest
FROM lake.gold.orders.snapshots;
```
