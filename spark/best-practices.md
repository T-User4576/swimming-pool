# Spark — Best Practices für unseren Lakehouse-Stack

Konkrete Empfehlungen für PySpark-Jobs gegen Iceberg/Lakekeeper auf S3,
deployed via Spark-Operator. Architektur-Grundlagen: [`./architecture.md`](./architecture.md).
K8s-Deployment-Mechanik: [`./kubernetes.md`](./kubernetes.md).

## 1. Sizing — Driver & Executor

### Executor

Faustregeln:

| Property | Empfehlung | Begründung |
|---|---|---|
| `executor.cores` | **4–5** pro Executor | Mehr Cores → JVM-GC-Pressure steigt überproportional, weniger HDFS/S3-Throughput pro Core. 4–5 ist der historisch validierte Sweet Spot. |
| `executor.memory` | **4–8 GB pro Core** | 16–40 GB pro Executor üblich. Bei sehr breiten Joins/Shuffles eher 8 GB/Core, bei reinen Scans 4 GB/Core. |
| `executor.memoryOverhead` | **10–20%** der `executor.memory`, mindestens 1 GB | Off-Heap (Tungsten, JNI, Python-Worker). Bei PySpark eher 20%, weil Python-Worker zusätzlichen Memory braucht. |
| `executor.instances` | nach Datenmenge | siehe Tabelle unten |

Initial-Sizing nach erwartetem Daten-Bewegungsvolumen (Read + Shuffle):

| Daten-Volumen | Cores total | RAM total |
|---|---|---|
| < 10 GB | 8–16 Cores | 32–64 GB |
| 10–100 GB | 20–40 Cores | 80–160 GB |
| 100 GB – 1 TB | 50–100 Cores | 200–400 GB |
| 1–10 TB | 100–500 Cores | 400 GB – 2 TB |

Praktisch: **pro 100 GB Shuffle ≈ 20–30 Cores planen**.

### Driver

Driver ist meistens **kleiner als gedacht** nötig — er führt nichts aus,
plant nur. Außer:

| Szenario | Driver-Sizing |
|---|---|
| Reine Mart-Pipeline (kein `collect()`) | 2 Cores / 8 GB reichen |
| `collect()` / `toPandas()` auf großen Result | so groß wie das gesammelte Result |
| Sehr viele Tasks (10k+) | mehr Memory wegen Task-State im Heap |
| Streaming | Pin auf eigene Node, gegen GC-Stalls absichern |

Bei OOM-Fehlern auf dem Driver fast immer `collect()` oder ähnliches im Code suchen — selten eine Sizing-Frage.

## 2. Iceberg-spezifische Spark-Configs

```yaml
sparkConf:
  # Iceberg + Lakekeeper REST
  spark.sql.extensions: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  spark.sql.catalog.lake: org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.lake.type: rest
  spark.sql.catalog.lake.uri: http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog
  spark.sql.catalog.lake.warehouse: main
  spark.sql.catalog.lake.io-impl: org.apache.iceberg.aws.s3.S3FileIO
  spark.sql.catalog.lake.s3.endpoint: http://minio.minio.svc.cluster.local:9000
  spark.sql.catalog.lake.s3.path-style-access: "true"
  spark.sql.defaultCatalog: lake

  # Vectorized Reads (default an, hier explizit)
  spark.sql.iceberg.vectorization.enabled: "true"

  # Adaptive Query Execution
  spark.sql.adaptive.enabled: "true"
  spark.sql.adaptive.coalescePartitions.enabled: "true"
  spark.sql.adaptive.skewJoin.enabled: "true"
  spark.sql.adaptive.autoBroadcastJoinThreshold: "100m"

  # Shuffle Partitions — AQE coalesced Down,
  # also ruhig hoch ansetzen für Skew-Toleranz
  spark.sql.shuffle.partitions: "400"

  # Iceberg-Writes: hash distribution für gleichmäßige Files
  spark.sql.iceberg.distribution-mode: hash

  # Serialisierung
  spark.serializer: org.apache.spark.serializer.KryoSerializer
```

## 3. S3 / MinIO Tuning

```yaml
hadoopConf:
  fs.s3a.endpoint: http://minio.minio.svc.cluster.local:9000
  fs.s3a.path.style.access: "true"
  fs.s3a.connection.ssl.enabled: "false"     # in-cluster, ohne TLS
  fs.s3a.fast.upload: "true"
  fs.s3a.threads.max: "20"
  fs.s3a.connection.maximum: "100"
  fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
```

**Wichtig zum Output-Committer**:

| Committer | Wann |
|---|---|
| **Iceberg-eigener Commit** (für Iceberg-Writes) | Default beim Schreiben in Iceberg-Tabellen — keine extra Konfiguration nötig. Iceberg committed atomar via Manifest-Snapshot, nicht via Hadoop-Committer. |
| **Magic Committer** (`spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2` + `s3a.committer=magic`) | Nur für **non-Iceberg** Parquet-Writes auf S3. Wenn ihr in Iceberg schreibt: **ignorieren**. |
| **Directory/File Committer** | Default v1 — auf S3 langsam (bis zu Stunden bei großen Mengen). Niemals auf S3 verwenden. |

In unserem Projekt: alle Writes gehen über Iceberg → Iceberg-Commit → kein
Hadoop-Committer-Tuning nötig.

## 4. Schreiben in Iceberg — Pattern

### Pattern A: Append (Bronze, Streaming-Output)

```python
df.writeTo("lake.bronze.events").append()
```

Iceberg fügt einen neuen Snapshot hinzu, alte Daten unverändert.

### Pattern B: Overwrite-Partition (Mart-Refresh)

```python
df.writeTo("lake.serving.orders_daily") \
  .overwritePartitions()
```

Überschreibt nur die Partitionen, die im DataFrame vorhanden sind. Andere
Partitionen bleiben. **Saubere Pattern für inkrementelle Mart-Loads**.

### Pattern C: MERGE INTO (CDC / Upsert)

```sql
MERGE INTO lake.silver.customers t
USING staging_updates s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Nutzt Iceberg V2-Deletes. Erzeugt Position-Delete-Files, die regelmäßig
compactet werden müssen (siehe [`../iceberg/maintenance.md`](../iceberg/maintenance.md)).

### Pattern D: Dynamic Partition Overwrite (Vorsicht!)

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").insertInto("lake.gold.orders")
```

Läuft, aber sub-optimal — geht nicht über Iceberg-API, sondern über die
Hadoop-Schreib-Schicht. **Pattern B ist immer vorzuziehen**.

## 5. Anti-Patterns

| Anti-Pattern | Warum schlecht | Stattdessen |
|---|---|---|
| `df.collect()` auf großen Datasets | Driver-OOM | Im Cluster aggregieren, nur Ergebnis-Zeilen sammeln |
| `count()` als Sanity-Check vor jedem Schritt | Triggert Job-Run nur für Zähler — teuer | `df.printSchema()`, im Spark-UI Stage-Größen prüfen |
| `coalesce(1).write(...)` für "ein File" | Kollabiert auf einen einzigen Task = ein Executor-Core | `repartition(N)` mit sinnvollem N, oder Iceberg-Compaction nachgeschaltet |
| RDD-API statt DataFrame | Catalyst greift nicht | DataFrame/SQL überall wo möglich |
| `spark.sql.shuffle.partitions = 200` blind beibehalten | Default ist für Mini-Datasets | Mit AQE meist OK, sonst hochsetzen für TB-Workloads |
| Python-UDF wo Built-in geht | UDF schlägt Vectorization → 10–100× langsamer | Built-in-Funktionen, `expr()`, nur im Notfall Pandas-UDF |
| `.cache()` reflexartig | Kostet Memory, oft nutzlos wenn DataFrame nur 1× verwendet | Nur cachen, wenn DataFrame mehrfach gebraucht wird |
| Wide Tables (1000+ Spalten) ohne Filter | Catalyst muss alles im Plan halten | `.select(...)` früh anwenden, Projection-Pushdown nutzen |
| `mode("overwrite")` auf Iceberg-Tabelle | Überschreibt **alle** Partitionen | `overwritePartitions()` (Pattern B) |

## 6. Performance-Debugging — Workflow

1. **Erstes Anzeichen**: Job läuft länger als erwartet oder OOM.
2. **Spark UI öffnen** (`port-forward` zum Driver-Pod, Port 4040, oder History Server).
3. **Stages-Tab**: welche Stage hängt? Skew? (max-task-time >> median-task-time)
4. **SQL-Tab**: Plan ansehen, ist Predicate-Pushdown drin? Broadcast vs Sort-Merge?
5. **Executors-Tab**: GC-Time-Anteil > 10%? → Memory zu klein. Tasks failed? → Logs.
6. **DAG-Visualization**: zu viele Shuffles? Stages konsolidieren.
7. **Logs in Loki**: Stack-Trace bei OOM/Exception.

Konkrete Symptome:

| Symptom | Ursache | Fix |
|---|---|---|
| Wenige Tasks dauern viel länger als andere | Daten-Skew | `spark.sql.adaptive.skewJoin.enabled = true`, oder Salt-Spalte zur Verteilung |
| Hoher GC-Time-Anteil | Executor-Memory zu klein | `executor.memory` hoch, ggf. `memoryOverhead` |
| Stage hat Tausende kleiner Tasks | zu viele Daten-Files (Iceberg Small-File-Problem) | `rewrite_data_files` (Iceberg-Maintenance) |
| Erste Stage scant alles, obwohl WHERE da | Predicate-Pushdown bricht (Cast in WHERE? Funktion auf Spalte?) | WHERE direkt auf Spalte, gleicher Datentyp |
| Driver-OOM | `collect()`/`toPandas()` im Code | Aggregation im Cluster |

## 7. Testen lokal vor K8s-Deployment

PySpark-Jobs lassen sich lokal mit MinIO + Lakekeeper im Docker-Compose
gegen Test-Tabellen testen, bevor sie als ConfigMap in den Cluster gehen:

```bash
docker compose up -d minio lakekeeper
spark-submit \
  --master local[4] \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.iceberg:iceberg-aws-bundle:1.6.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=rest \
  --conf spark.sql.catalog.lake.uri=http://localhost:8181/catalog \
  job.py
```

Das vermeidet, dass jeder Iterations-Zyklus über `kubectl create configmap` +
`argo submit` geht.

## 8. Reproduzierbarkeit — pinnen, was sich pinnen lässt

| Was | Wo gepinnt |
|---|---|
| Spark-/Runner-Image-Tag | konkrete Version, nicht `:latest` |
| Iceberg-Version | `iceberg-spark-runtime-3.5_2.12:1.6.0` |
| Python-Dependencies | `requirements.txt` mit `==`-Versionen (Distribution je nach Pattern: venv-pack-Archiv, ConfigMap, Image — siehe `kubernetes.md` Abschnitt 5) |
| Operator-Version | Helm-Chart-Version in Argo-CD/IaC |

`:latest` und floating Versions sind in Maintenance-Pipelines die häufigste
Quelle für "warum ist der Job heute kaputt" — vermeiden.

## 9. Zusammenfassung der Defaults für unseren Stack

Wenn du einen neuen Spark-Job für dieses Projekt aufsetzst, starte mit
diesen Werten und tune danach gegen reale Last:

```yaml
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: <runner-image>:<gepinnte-version>   # Runner-Image — Variante je nach Code-Distribution-Pattern
  mainApplicationFile: <pfad-zur-job-datei>  # je nach Pattern: ConfigMap-Mount, venv-Archiv, S3-URL, ...
  sparkConf:
    spark.sql.extensions: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    spark.sql.catalog.lake: org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.lake.type: rest
    spark.sql.catalog.lake.uri: http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog
    spark.sql.defaultCatalog: lake
    spark.sql.adaptive.enabled: "true"
    spark.sql.shuffle.partitions: "400"
    spark.sql.iceberg.distribution-mode: hash
    spark.eventLog.enabled: "true"
    spark.eventLog.dir: s3a://spark-event-logs/
    spark.serializer: org.apache.spark.serializer.KryoSerializer
  driver:
    cores: 2
    memory: 8g
    serviceAccount: spark-driver
  executor:
    cores: 4
    instances: 5
    memory: 16g
  restartPolicy:
    type: OnFailure
    onFailureRetries: 2
  timeToLiveSeconds: 3600
```

Variationen davon sind gut. Strukturelle Abweichungen (RDD statt DataFrame,
`collect()` im Code, kein AQE) sollten begründet werden.
