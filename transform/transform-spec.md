# Transform-Spec — Schema-Referenz

Eine Pipeline-Spec ist ein deklaratives YAML-Dokument, das eine "einfache"
Datenverarbeitung vollständig beschreibt: **woher** die Daten kommen (`source`),
**welches Schema** sie haben (`schema`), **was** mit ihnen passiert (`steps`)
und **wohin** das Ergebnis geschrieben wird (`sink`). Der generische Runner
`spark/runner.py` führt sie aus — es wird kein pipeline-spezifischer Code
geschrieben.

Specs liegen in [`./pipelines/`](./pipelines/). Eine neue Verarbeitung = eine
neue Spec-Datei, kein Eingriff in `runner.py` oder `argo/transform-workflow.yaml`.

---

## 1. Aufbau

```yaml
name:         <string>     # Pflicht. Konvention: <layer>-<domain>-<verb>
description:  <string>     # optional, Freitext (deutsch)
source:       { ... }      # Pflicht
schema:       { ... }      # optional (Pflicht bei source.type=kafka)
steps:        [ ... ]      # optional, geordnete Liste
sink:         { ... }      # Pflicht
```

Ausführungsreihenfolge: `source` lesen → `schema` anwenden → `steps` der Reihe
nach → `sink` schreiben.

---

## 2. `source`

Das Feld `type` wählt einen Source-Handler aus der `SOURCES`-Registry. v1
kennt drei Typen.

### `type: iceberg`
Liest eine Iceberg-Tabelle aus dem Lakekeeper-Catalog.

```yaml
source:
  type: iceberg
  table: lake.bronze.orders_raw     # Pflicht, voll qualifiziert
  snapshot: 6821837374243321        # optional, Snapshot-ID (Time-Travel)
```

### `type: file`
Liest eine oder mehrere Dateien aus S3 (MinIO).

```yaml
source:
  type: file
  path: s3a://landing/orders/        # Pflicht (Datei oder Prefix)
  format: csv                        # csv | json | parquet | orc | text
  options:                           # optional, Spark-Reader-Optionen
    header: "true"
    delimiter: ";"
    multiLine: "true"
```

- **Kompression**: `.gz`, `.bz2`, `.lz4`, `.deflate`, `.zstd` u.a. werden
  automatisch an der Datei-Endung erkannt — keine Extra-Option nötig. Fehlt die
  Endung, `options.compression` explizit setzen. `parquet`/`orc` tragen ihre
  Kompression intern.
- Bei `csv`/`json` wird der `schema`-Block, falls vorhanden, als Read-Schema
  genutzt (vermeidet teure Schema-Inferenz).

### `type: kafka`
**Bounded Batch-Read** eines Topics über einen festen Offset-Bereich — kein
kontinuierlicher Consumer (siehe [Abschnitt 7](#7-bewusste-auslassungen)).

```yaml
source:
  type: kafka
  bootstrap_servers: kafka.kafka.svc.cluster.local:9092   # Pflicht
  topic: orders                                            # Pflicht
  starting_offsets: earliest        # optional, default earliest
  ending_offsets: latest            # optional, default latest
  value_format: json                # json (v1) | avro (noch nicht implementiert)
  options:                          # optional, kafka.*-Optionen (SASL/TLS ...)
    kafka.security.protocol: SSL
```

Der `schema`-Block ist bei `kafka` **Pflicht**: Die binäre `value`-Payload wird
damit via `from_json` deserialisiert. `value_format: avro` ist in v1 nicht
implementiert (siehe Abschnitt 7).

---

## 3. `schema`

Optional bei `iceberg`/`file`, **Pflicht** bei `kafka`. Beschreibt das erwartete
Eingangs-Schema und wird je nach Source unterschiedlich genutzt:

- `iceberg` — Validierung/Enforcement des gelesenen DataFrames.
- `file` (csv/json) — zusätzlich als Read-Schema.
- `kafka` — als Deserialisierungs-Schema der Payload.

```yaml
schema:
  mode: enforce                      # validate | enforce
  on_extra_columns: ignore           # ignore (default) | error  — nur mode=validate
  on_missing_columns: error          # error (default) | null    — nur mode=enforce
  columns:
    - { name: order_id, type: bigint,         nullable: false }
    - { name: order_ts, type: timestamp,      nullable: false }
    - { name: amount,   type: "decimal(12,2)" }
```

- **`mode: validate`** — prüft, dass jede Soll-Spalte mit passendem Typ
  existiert; bricht bei Abweichung ab (`raise`). `nullable: false`-Verstöße
  werden als Warnung geloggt (lese-seitige Nullability ist nicht zuverlässig
  erzwingbar). `on_extra_columns: error` lässt zusätzlich unerwartete Spalten
  scheitern.
- **`mode: enforce`** — `select` auf die Soll-Spalten (Reihenfolge inklusive) +
  `cast` auf die Soll-Typen. Extra-Spalten werden verworfen. Fehlende Spalten →
  Abbruch (`on_missing_columns: error`) oder als `NULL`-Spalte ergänzt (`null`).
- **`type`** sind Spark-DDL-Typen (`bigint`, `integer`, `string`, `timestamp`,
  `date`, `decimal(p,s)`, `double`, `boolean`, …). Gängige Aliase (`long`,
  `int`, `numeric`, …) werden normalisiert.

---

## 4. `steps`

Geordnete Liste; jeder Eintrag hat ein `type` aus der `STEPS`-Registry. Steps
werden sequentiell auf den DataFrame angewandt. `steps` darf leer/weggelassen
werden (reine Source→Sink-Kopie, z.B. Format-Konvertierung).

### `rename` — Spalten umbenennen (Spalten-Mapping)
```yaml
- type: rename
  columns: { KUNDE_ID: customer_id, BETRAG: amount }
```

### `select` — Projektion
```yaml
- type: select
  columns: [order_id, customer_id, amount]
```

### `cast` — Typ-Konvertierung
```yaml
- type: cast
  columns: { amount: "decimal(12,2)", order_ts: timestamp }
```

### `filter` — Zeilen filtern
```yaml
- type: filter
  where: "status <> 'CANCELLED' AND amount > 0"   # Spark-SQL-Expression
```

### `derive` — berechnete Spalten
```yaml
- type: derive
  columns:
    order_day:  "cast(order_ts as date)"
    net_amount: "amount * (1 - discount_rate)"
```

### `dedup` — Deduplizierung
```yaml
- type: dedup
  keys: [order_id]
  order_by:                          # optional
    - { column: updated_at, dir: desc }
    - { column: ingest_ts,  dir: desc }
```
- **ohne `order_by`** → `dropDuplicates(keys)`: behält eine beliebige Zeile je
  Key. Billiger (keine Window-Sort).
- **mit `order_by`** → `row_number()` über `Window.partitionBy(keys)`: behält
  deterministisch die *erste* Zeile je Key gemäß Sortierung (z.B. die neueste
  Version bei `dir: desc` auf einem Update-Zeitstempel).

### `aggregate` — Gruppierung + Aggregation
```yaml
- type: aggregate
  group_by: [order_day, customer_id]   # Spalten oder Expressions
  aggregations:
    - { column: amount,   func: sum,   as: revenue }
    - { column: order_id, func: count, as: order_count }
    - { column: amount,   func: avg,   as: avg_ticket }
```
Erlaubte `func`-Werte: `sum`, `count`, `count_distinct`, `avg`, `min`, `max`,
`first`, `last`.

### `sql` — Escape-Hatch
Freie Spark-SQL gegen den bisherigen Zwischenstand, der als TempView `_in`
registriert ist.
```yaml
- type: sql
  query: "SELECT *, amount / nullif(order_count, 0) AS ratio FROM _in"
```
> Sobald eine Pipeline überwiegend aus `sql`-Steps besteht, ist das das Signal,
> ein echtes Transformations-Tool (dbt/SQLMesh) einzuführen — siehe Abschnitt 7.

---

## 5. `sink`

Das Feld `type` wählt einen Sink-Handler aus der `SINKS`-Registry. v1 kennt
nur `iceberg`.

```yaml
sink:
  type: iceberg
  table: lake.silver.orders          # Pflicht, voll qualifiziert
  mode: overwrite_partitions         # append | overwrite_partitions | merge
  merge_keys: [order_id]             # Pflicht bei mode=merge
  write_options:                     # optional, Iceberg-Write-Optionen
    fanout-enabled: "true"
  create_if_not_exists: false        # optional, default false
```

| `mode` | Verhalten |
|---|---|
| `append` | `df.writeTo(table).append()` — hängt an. |
| `overwrite_partitions` | `df.writeTo(table).overwritePartitions()` — überschreibt nur die im DataFrame vorkommenden Partitionen. Für idempotente Mart-Loads bevorzugt. |
| `merge` | `MERGE INTO` über `merge_keys`: `WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`. Upsert. |

- **`create_if_not_exists`** (default `false`): Die Sink-Tabelle wird per DDL
  vorab und menschlich versioniert angelegt — so trägt sie die kuratierten
  TBLPROPERTIES aus [`../iceberg/table-design.md`](../iceberg/table-design.md)
  (Compression, Target File Size, Metrics, Maintenance-Properties). Bei `true`
  legt der Runner eine fehlende Tabelle **unpartitioniert** an; in Kombination
  mit `partitioned_by` bricht er bewusst ab.
- `merge` erzeugt Position-Deletes — die Sink-Tabelle braucht dann
  `maintenance.position_deletes.enabled=true` (siehe
  [`../iceberg/maintenance.md`](../iceberg/maintenance.md)).
- Pattern D (`insertInto` / dynamic partition overwrite) wird **nicht**
  angeboten — `overwrite_partitions` ist immer vorzuziehen
  (`../spark/best-practices.md`).

---

## 6. Erweiterbarkeit

Source, Step und Sink sind je eine Registry in `spark/runner.py`
(`SOURCES`, `STEPS`, `SINKS`) — dasselbe Muster wie das `JOBS`-Dict in
`iceberg/spark/maintenance.py`. **Ein neuer Typ = eine Handler-Funktion + ein
Registry-Eintrag**, ohne Eingriff in `main()` oder bestehende Handler. So
lassen sich später z.B. ein JDBC-Source-Typ oder ein Datei-Export-Sink additiv
ergänzen.

---

## 7. Bewusste Auslassungen

| Thema | Status / Begründung |
|---|---|
| Inkrementelle Verarbeitung | v1 ist reines Full-Refresh-Batch. Zeitfenster über einen `filter`-Step abbildbar. Ein deklarativer `source.incremental`-Block (Zeitfenster bzw. `Trigger.AvailableNow` + Checkpoint) ist Folge-Iteration. |
| Kafka als Streaming-Source | Bewusst nur **bounded** Batch-Read (fester Offset-Bereich). Kontinuierliches Kafka→Iceberg-Streaming bleibt Flink-Rolle (AGENTS.md §2). Der Runner darf nicht zum Flink-Ersatz ausgebaut werden. |
| `value_format: avro` | Nicht in v1 — `from_avro` braucht ein Avro-JSON-Schema und das `spark-avro`-Package. v1 deckt `json` ab. |
| Pattern D (`insertInto`) | Nicht angeboten — `overwrite_partitions` ist sauberer. |
| Sink-seitige Schema-Validierung | v1 prüft nur das Eingangs-Schema. Validierung des Ergebnis-Schemas gegen die Soll-Tabelle ist offener Punkt. |
| `join` zwischen mehreren Sources | v1 hat genau eine Source. Joins nur über den `sql`-Escape-Hatch gegen voll qualifizierte Tabellen. |
| dbt / SQLMesh / Spark-4 Declarative Pipelines | Der Spec-Runner ist bewusst minimal. Sobald der SQL-Anteil dominiert, ist ein echtes Tool fällig — das flache Spec-Schema ist absichtlich mechanisch dorthin übersetzbar. |

---

## 8. Verifikations-Checkliste

Das Repo hat keine Test-Suite — Verifikation erfolgt über Dry-Run und gezielte
Queries (analog `../iceberg/spark/maintenance.py --dry-run`).

1. **Spec-Lint lokal**: `spark-submit --master local[2] spark/runner.py
   --catalog lake --spec pipelines/<name>.yaml --dry-run` gegen ein lokales
   MinIO/Lakekeeper (`../spark/best-practices.md`). Fängt Schema-, Step-Typ-
   und Spalten-Fehler vor jedem Cluster-Lauf.
2. **Cluster-Dry-Run**: `argo submit --from cronwf/transform-gold-revenue-daily
   -n argo -p dry-run=true`. Im Driver-Log stehen `explain`-Plan,
   Ergebnis-Schema und Zeilenzahl. Prüfen: Predicate-Pushdown beim
   Source-Scan vorhanden? Kein neuer Snapshot in der Sink-Tabelle:
   `SELECT count(*) FROM lake.gold.revenue_daily.snapshots
   WHERE committed_at > current_timestamp - interval 1 hour;` → erwartet `0`.
3. **Echter Lauf**: derselbe Workflow ohne `dry-run`. Danach Aggregat-
   Konsistenz prüfen (Gold-Summe vs. gefilterte Silver-Summe).
4. **Idempotenz** (bei `overwrite_partitions`): zweiter Lauf ohne
   Source-Änderung → identisches Ergebnis, `added-records` des zweiten
   Snapshots gleich dem des ersten.
5. **Dedup**: `SELECT <key>, count(*) c FROM <sink> GROUP BY <key>
   HAVING c > 1;` → erwartet `0` Zeilen.
