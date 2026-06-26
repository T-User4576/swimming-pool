# Inkrementelle Verarbeitung & Wiederaufsetz-Sicherheit

Wie ein cron-getriggerter Spark-Job nach einem Crash weiß, was er schon
verarbeitet hat — ohne eigene Bookmark-Logik. Pattern: **Spark Structured
Streaming mit `Trigger.AvailableNow`**, läuft als Batch-Job mit Streaming-
State-Mechanik darunter.

## Problem

Klassischer Cron-Spark-Job liest Source X → schreibt Mart Y. Wenn er bei
Iteration N abbricht, muss Iteration N+1 wissen, was N schon committed hat,
um nichts doppelt oder lückenhaft zu verarbeiten.

Drei Lösungsansätze:

| Pattern | Wie | Wann |
|---|---|---|
| **A. Idempotent Overwrite** | Job bestimmt sein Zeitfenster deterministisch aus dem Trigger-Zeitpunkt; schreibt mit `overwritePartitions()`; Retry produziert dasselbe Ergebnis | zeitbasierte Marts, kein late-arriving-Problem |
| **B. Bookmark-Tabelle** | Eigene Iceberg-Tabelle trackt `last_processed_snapshot_id` pro Job | komplexere Sources, manuelle Kontrolle |
| **C. `Trigger.AvailableNow` + Streaming-Checkpoint** | Streaming-Framework verwaltet den Bookmark im Checkpoint-Pfad | inkrementelle Iceberg→Iceberg-Pipelines, late-arriving möglich |

A ist der pragmatische Default. **C ist der Fokus dieses Dokuments.**

## Wichtige Klarstellung: `Trigger.AvailableNow` ist kein Dauerbetrieb

| | Continuous Streaming | `Trigger.AvailableNow` | Pure Batch |
|---|---|---|---|
| Lebensdauer | Pod läuft permanent | Startet, arbeitet ab, **exit 0** | Startet, arbeitet ab, exit |
| Schedule | n.a. | Argo CronWorkflow | Argo CronWorkflow |
| Wer trackt Fortschritt | Streaming-Checkpoint | Streaming-Checkpoint | du selbst |

Du startest den Job stündlich (oder so) via Argo. Pod existiert nur für
die Dauer der Verarbeitung — Sekunden bis Minuten. Argo bewertet nach
Exit-Code. Aus operativer Sicht **identisch zu einem Batch-Job**, nur dass
der Job seinen eigenen "Wo war ich"-Zustand kennt.

## Anti-Konzern: das Small-File-Problem

Streaming-Iceberg-Writes sind berüchtigt für viele kleine Files. Das Problem
hängt aber nicht am Streaming-API, sondern an der **Anzahl der Sink-Commits
pro Run**:

| Setup | Sink-Commits/Tag | Bewertung |
|---|---|---|
| Continuous Stream, `processingTime="30s"` | ~2.880 | schlecht |
| `Trigger.AvailableNow` stündlich, ungebremst, viele Source-Snapshots | je nach Quelle 24–500+ | mittel |
| `Trigger.AvailableNow` stündlich, alles in 1 Micro-Batch | **24** | strukturell wie Pure Batch |

Die Hebel für "1 Commit pro Run":
- **`max-files-per-micro-batch`** auf der Iceberg-Source hochsetzen (oder weglassen)
- **`maxFilesPerTrigger` / `maxRecordsPerTrigger` NICHT setzen**
- Sink-Properties: `write.target-file-size-bytes = 536870912`, `write.distribution-mode = hash`

## Beispiel-Code

```python
(spark.readStream.format("iceberg")
   .option("max-files-per-micro-batch", "100000")    # praktisch unlimitiert
   # KEINE option("maxFilesPerTrigger", ...) setzen
   .load("lake.bronze.orders")
   .groupBy("day", "customer_id")
   .agg(...)
   .writeStream
   .format("iceberg")
   .outputMode("append")
   .option("checkpointLocation", "s3a://lakehouse-checkpoints/gold_orders_daily/")
   .option("fanout-enabled", "true")                  # falls partitionierter Sink
   .trigger(availableNow=True)
   .toTable("lake.gold.orders_daily"))
```

Drei Punkte zu diesem Snippet:
- **`.toTable("lake.gold.orders_daily")`** ist die saubere Spark-3.1+ API. Nicht
  `.option("path", ...).start()` verwenden — Letzteres ist ambig (Pfad vs.
  Tabellen-Identifier).
- **Checkpoint-Pfad ist eindeutig pro Job.** Niemals zwei Streams auf den
  gleichen Pfad zeigen lassen.
- **Bei Schema-/Code-Änderungen** kann der Checkpoint inkompatibel werden →
  gezielt löschen, mit Daten-Wiederholung als Konsequenz.

## Was ein Run intern tut

1. Job startet → liest `checkpointLocation` → "letzter verarbeiteter Iceberg-Snapshot war X"
2. Source-Iceberg-Reader liefert alle Snapshots seit X als ein Micro-Batch
   (dank großem `max-files-per-micro-batch`)
3. DAG läuft, Sink committed **eine** Iceberg-Snapshot-Transaktion
4. Streaming-Framework updated den Checkpoint **atomar mit dem Sink-Commit**
5. Stream beendet sich → Job exit 0 → Argo zufrieden

Bei Crash zwischen Schritt 3 und 4: kein Datenverlust, kein Dupe — der
nächste Run sieht den unveränderten Checkpoint und liest ab Snapshot X erneut.

## Operative Voraussetzungen

| Punkt | Anforderung |
|---|---|
| Spark-Version | **≥ 3.3** (für `Trigger.AvailableNow`) |
| Iceberg-Streaming-Source | unterstützt; Property-Namen je Iceberg-Version unterschiedlich |
| Checkpoint-Storage | dedizierter S3-Bucket (oder Sub-Pfad), strong consistency |
| Sink-Tabelle | muss existieren (Streaming-Sink legt nicht automatisch an) |

### Checkpoint-Bucket aus Iceberg-Maintenance ausnehmen

Der Checkpoint-Bucket darf nicht von `remove_orphan_files` (siehe
[`../iceberg/maintenance.md`](../iceberg/maintenance.md) Abschnitt 4)
angefasst werden. Drei Wege:

- Eigener Bucket nur für Checkpoints, in der Maintenance-Pipeline gar nicht gelistet
- Wenn im selben Bucket: Iceberg-Tabellen-Locations und Checkpoint-Pfade
  klar trennen, Maintenance via TBLPROPERTIES nur auf Iceberg-Tabellen
- Sub-Pfad-Konvention im Bucket (`<bucket>/iceberg/...` vs `<bucket>/checkpoints/...`)

Wenn `remove_orphan_files` einen Checkpoint-File löscht, ist der Stream
beim nächsten Start kaputt — Recovery-Aufwand groß. Vorher organisatorisch
trennen.

## Verifikation

Drei voneinander unabhängige Checks. Erst der grobe (Cron läuft, Frequenz
passt), dann die feinen (es werden wirklich nur neue Daten verarbeitet).

### Check 1: Anzahl Sink-Commits passt zur Trigger-Frequenz

Nach 24h Betrieb:

```sql
SELECT count(*) AS commits, max(committed_at) AS last_commit
FROM lake.gold.orders_daily.snapshots
WHERE committed_at > current_timestamp - interval 1 day;
```

| `commits` | Bewertung |
|---|---|
| ~24 (= 1 pro stündlichem Run) | korrekt konfiguriert |
| > 50 | zu viele Micro-Batches — `max-files-per-micro-batch` zu klein, Limits irgendwo gesetzt |
| < 24 | Job läuft nicht regelmäßig — Argo Cron prüfen |

Sagt aber **nichts** darüber aus, ob wirklich nur neue Daten gelesen werden —
ein fehlerhaft konfigurierter Stream könnte auch jedes Mal alles re-verarbeiten
und trotzdem 24 Commits erzeugen.

### Check 2: Checkpoint-Offsets wandern monoton mit der Source

Die `offsets/N`-Files im Checkpoint-Verzeichnis enthalten die Source-Position,
die nach Micro-Batch N erreicht war. Bei korrektem Inkrement-Verhalten muss
sich die `snapshotId` darin nach jedem Run **erhöhen** und nie zurückspringen.

```bash
# (mc = MinIO Client, vorher 'mc alias set lake http://minio... <key> <sec>')

# Liste der Offsets — pro Micro-Batch eine Datei
mc ls lake/lakehouse-checkpoints/gold_orders_daily/offsets/

# Inhalt der jeweils letzten Offsets ausgeben
mc cat lake/lakehouse-checkpoints/gold_orders_daily/offsets/N
```

Inhalt sieht so aus (Iceberg-Source, vereinfacht):

```json
v1
{
  "batchWatermarkMs": 0,
  "batchTimestampMs": 1715166000000,
  ...
}
{
  "lake.bronze.orders":
    {"snapshotId": 8742356129387465, "position": 0, "scanAllFiles": false}
}
```

Die `snapshotId` mit den Snapshots der Source-Tabelle abgleichen:

```sql
-- Liste der zuletzt geschriebenen Snapshots der Source
SELECT snapshot_id, parent_id, committed_at, summary['operation'] AS op
FROM lake.bronze.orders.snapshots
ORDER BY committed_at DESC
LIMIT 10;
```

Erwartetes Verhalten:
- `snapshotId` in `offsets/N+1` ist > als in `offsets/N`
- Werte korrespondieren zu echten Snapshot-IDs der Bronze-Tabelle
- Niemals `scanAllFiles: true` nach dem ersten Lauf (das hieße: alles neu scannen)

### Check 3: Source-Snapshot-Range ↔ Sink-Snapshot-Range

Iceberg trackt im Snapshot-Summary, woher die Daten kamen. Damit kann man
direkt vergleichen, welche Bronze-Snapshots in welchen Mart-Snapshot eingeflossen
sind:

```sql
-- Sink: was wurde wann committed?
SELECT
  snapshot_id,
  committed_at,
  summary['added-records']    AS added_rows,
  summary['added-data-files'] AS added_files
FROM lake.gold.orders_daily.snapshots
ORDER BY committed_at DESC
LIMIT 5;
```

Pro Sink-Commit prüfen, dass `added-records` plausibel zur Datenmenge passt,
die in der Source seit dem vorigen Run hinzugekommen ist:

```sql
-- Source: wie viele Rows wurden im selben Zeitfenster hinzugefügt?
SELECT
  sum(cast(summary['added-records'] AS bigint)) AS source_added_rows
FROM lake.bronze.orders.snapshots
WHERE committed_at BETWEEN
  TIMESTAMP '2026-05-08 09:00:00' AND TIMESTAMP '2026-05-08 10:00:00';
```

Wenn `source_added_rows` ≈ `added_rows` im Sink (modulo Aggregations-/Filter-
Logik des Jobs), ist die Inkrement-Logik intakt. Wenn das Sink-`added_rows`
deutlich größer als das Source-Delta ist, wird mehr verarbeitet als nötig —
typisch nach einer Checkpoint-Korruption oder versehentlichem `scanAllFiles`.

### Check 4: Kontrolliertes Experiment (einmalig zur Inbetriebnahme)

Definitiver Beweis durch Provokation:

1. **Stream einmal komplett leeren laufen lassen** bis offsets stabil sind.
2. **Bekannte Daten in Bronze einfügen**, z.B. genau 1.000 Test-Rows mit
   eindeutigem Marker:
   ```sql
   INSERT INTO lake.bronze.orders
   SELECT ... FROM ... WHERE customer_id = 999999;   -- Marker
   ```
3. **Trigger einen Job-Run** (Argo manuell submitten).
4. **Im Sink prüfen**, ob exakt die erwarteten 1.000 Rows aus dem Marker
   verarbeitet wurden, und nichts darüber hinaus:
   ```sql
   SELECT count(*) FROM lake.gold.orders_daily WHERE customer_id = 999999;
   ```
5. **Zweiten Run ohne neue Bronze-Daten** triggern. Sink-Snapshot-Anzahl darf
   sich **nicht** erhöhen (oder nur um einen "leeren" Snapshot, je nach
   Iceberg-Sink-Verhalten).

Schritt 5 ist der härteste Test: kommt ohne neue Daten ein neuer Sink-Snapshot
mit `added-records > 0`, wird re-verarbeitet — Konfiguration kaputt.

### Check 5 (laufendes Monitoring): StreamingQueryListener

Im Prod-Job einen `StreamingQueryListener` aktivieren, der pro Micro-Batch
Source-Offsets und Row-Counts in ein Audit-Log schreibt:

```python
class AuditListener(StreamingQueryListener):
    def onQueryProgress(self, event):
        progress = event.progress
        # progress.sources[0]['startOffset'] und ['endOffset'] enthalten
        # die Iceberg-Snapshot-IDs der jeweiligen Position
        log.info(f"batch={progress.batchId} "
                 f"source={progress.sources[0]['startOffset']}→{progress.sources[0]['endOffset']} "
                 f"rows={progress.numInputRows}")

spark.streams.addListener(AuditListener())
```

Output landet in den Pod-Logs (also im Log-Backend), aggregierbar per Job-Name.
Wenn `numInputRows` über mehrere Runs hinweg auf demselben Niveau bleibt
obwohl in der Source nichts mehr passiert, läuft der Stream nicht inkrementell.

## Wann `Trigger.AvailableNow` nicht sinnvoll ist

| Szenario | Stattdessen |
|---|---|
| Maintenance-Jobs (Compaction, Snapshot-Expiry) | Pure Batch — kein "Wo war ich"-Problem |
| Marts mit deterministischem Zeitfenster, kein late-arriving | Pattern A (Idempotent Overwrite) — einfacher |
| Sehr seltene Source-Updates (1× pro Tag) | Pure Batch + Filter — Streaming-Overhead lohnt nicht |
| Quelle ist keine Iceberg-Tabelle (z.B. Kafka) | Dann ohnehin Streaming, ggf. Continuous statt AvailableNow |

## Verifikation gegen Iceberg-Doku

Die Streaming-Source-Optionen für Iceberg (`max-files-per-micro-batch`
etc.) haben sich zwischen Versionen mehrfach geändert. Vor Production-
Roll-out gegen `iceberg.apache.org/docs/<gepinnte-version>/spark-structured-streaming/`
verifizieren — Property-Namen und Defaults können sich verschoben haben.
