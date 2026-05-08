# Iceberg — Tabellen-Design für performante Cold Reads

Ziel: auch nicht-gecachte Queries (z.B. ad-hoc auf historische Partitionen)
sollen Sub-10s statt Sub-Minute brauchen. Hebel sind ausschließlich auf der
Schreibseite — wer zur Query-Zeit optimieren will, ist zu spät.

## 1. Die fünf Hebel im Überblick

| Hebel | Wirkung | Wer kümmert sich drum |
|---|---|---|
| **Hidden Partitioning** | Partition Pruning ohne Filter-Hacks | Schreib-Pipeline (Spark/Flink) |
| **Sort Order** | File-Skipping via Min/Max | Schreib-Pipeline + Compaction |
| **File-Größe** | weniger File-Opens, mehr Parallelismus | Schreib-Pipeline + Compaction |
| **Manifest-Größe** | schnellerer Plan-Step | Compaction (`rewriteManifests`) |
| **Column-Stats / Bloom** | manifest-level Skipping | Schreib-Pipeline + Properties |

## 2. Partitionierung

### Hidden Partitioning verwenden, nicht explizit

```sql
-- Gut: Iceberg verwaltet die Partition-Spalte intern
CREATE TABLE lake.gold.orders (
  order_id    bigint,
  order_ts    timestamp,
  customer_id bigint,
  amount      decimal(12,2)
)
USING iceberg
PARTITIONED BY (days(order_ts));

-- Schlecht: explizite day-Spalte, doppelte Datenhaltung, Filter muss umgebaut werden
CREATE TABLE ... PARTITIONED BY (day);
```

Filter-Queries wie `WHERE order_ts >= '2026-01-01'` triggern automatisch Partition-Pruning — kein `WHERE day = '...'` Boilerplate nötig.

### Transformationen (Auswahl)

| Transform | Wann | Beispiel |
|---|---|---|
| `years(ts)` | Sehr langsame Daten, lange Historie | Audit-Logs |
| `months(ts)` | Reporting-Tabellen | Finance |
| `days(ts)` | **Default für Time-Series** | Orders, Events |
| `hours(ts)` | Sehr hohe Schreibrate, kurze SLA | IoT, CDC-Streams |
| `bucket(N, col)` | Hash-Partitionierung für gleichmäßige Verteilung | `bucket(32, customer_id)` |
| `truncate(N, col)` | String-Prefix-Partitionierung | Region-Codes |

### Faustregel: Partition-Cardinality

- **Ziel: 100 MB – 10 GB Daten pro Partition**
- < 100 MB: zu fein, Manifest-Inflation
- > 10 GB: zu grob, Pruning bringt wenig

Bei Mehrfach-Partitionierung (z.B. `days(ts) + bucket(16, tenant)`) Cardinality multiplikativ rechnen.

### Partition Spec Evolution

Iceberg erlaubt Partition-Spec-Änderungen ohne Datenrewrite:
```sql
ALTER TABLE lake.gold.orders REPLACE PARTITION FIELD days(order_ts) WITH hours(order_ts);
```
Alte Daten bleiben in alter Partitionierung, neue Daten in neuer — Reads funktionieren weiter, Compaction kann später angleichen.

## 3. Sort Order

```sql
ALTER TABLE lake.gold.orders WRITE ORDERED BY (order_ts, customer_id);
```

Wirkung: Innerhalb jedes Files sind Daten nach `(order_ts, customer_id)` sortiert. Iceberg legt Min/Max-Stats pro Spalte ab → Predicates wie `WHERE customer_id = 12345` können ganze Files überspringen, auch wenn die Tabelle nicht nach `customer_id` partitioniert ist.

**Reihenfolge der Sort-Spalten matters**: erste Spalte = stärkstes Skipping.

Bei Compaction (`rewriteDataFiles`) wird der Sort-Order **angewendet** — daher entscheidend, dass Compaction läuft (siehe `maintenance.md`).

## 4. File-Größe

Default ist oft 128 MB — bei euren TB/Tag fast immer zu klein.

```sql
ALTER TABLE lake.gold.orders SET TBLPROPERTIES (
  'write.target-file-size-bytes' = '536870912',          -- 512 MB
  'write.parquet.row-group-size-bytes' = '134217728'     -- 128 MB Row Groups
);
```

Sweet Spot: **256–512 MB** Files mit **64–128 MB** Parquet-Row-Groups. Größer = besseres Compression-Verhältnis & weniger File-Opens. Kleiner = mehr Parallelismus, aber Overhead frisst es.

## 5. Bloom Filters & Column Stats

```sql
ALTER TABLE lake.gold.orders SET TBLPROPERTIES (
  'write.parquet.bloom-filter-enabled.column.customer_id'   = 'true',
  'write.parquet.bloom-filter-enabled.column.order_id'      = 'true',
  'write.metadata.metrics.default'                          = 'truncate(16)',
  'write.metadata.metrics.column.amount'                    = 'full'
);
```

| Property | Wann |
|---|---|
| Bloom-Filter pro Spalte | Equality-Filter auf High-Cardinality-Spalten (`customer_id = ...`) |
| `metrics.default = truncate(16)` | Default: nur erste 16 Bytes für String-Stats — günstig |
| `metrics.column.X = full` | Volle Min/Max für Spalten, die häufig in Range-Filtern stehen |
| `metrics.column.X = none` | Stats deaktivieren für riesige String-Spalten (Memo, Description) |

### Wo Metriken gespeichert werden

In den **Manifest-Files** (Avro), pro Daten-File ein Eintrag mit `value_counts`,
`null_value_counts`, `lower_bounds`, `upper_bounds` als `Map<field_id, ByteBuffer>`.
Diese Maps werden bei jedem Query-Plan-Step deserialisiert — große Bounds =
langsamerer Plan + mehr Heap.

| Mode | Inhalt | Bytes pro File pro Spalte |
|---|---|---|
| `none` | nichts | 0 |
| `counts` | nur counts/null/nan | ~16 |
| `truncate(N)` | counts + Min/Max auf N Bytes gecuttet | ~16 + 2×N |
| `full` | counts + Min/Max ungeschnitten | ~16 + 2×sizeof(value) |

Für Numerics/Dates/Timestamps ist `full` praktisch kostenlos (fix-size).
Für String-/Binary-Spalten kann `full` Manifests aufblähen — bei 100k Files
mit einer 2 KB-JSON-Spalte schon ~400 MB Manifest-Volume.

### Empfehlung pro Spalten-Typ

| Spalten-Typ | Empfohlene Mode | Begründung |
|---|---|---|
| Numerics, Dates, Timestamps | `full` | Fix-size, kostet nichts, Range-Filter profitieren |
| ID-Spalten (`bigint`, kurze UUID) | `full` | Equality-Filter, kostet nichts |
| Status/Enum-Strings (≤ 16 Zeichen) | `full` oder `truncate(32)` | Klein, Filter-relevant |
| Email/URL/Phone | `truncate(16)` | Prefix-Skipping reicht |
| Free-Text (Memo, Description) | `none` oder `counts` | Kein Range-Filter, Bounds wären riesig |
| JSON-Blobs / serialisierte Daten | `none` | Min/Max auf JSON ist sinnlos |
| Binary / BLOB | `none` | Niemand filtert auf Binary-Bounds |

Konkret:

```sql
ALTER TABLE lake.gold.orders SET TBLPROPERTIES (
  'write.metadata.metrics.default'                = 'truncate(16)',
  'write.metadata.metrics.column.order_id'        = 'full',
  'write.metadata.metrics.column.customer_id'     = 'full',
  'write.metadata.metrics.column.order_ts'        = 'full',
  'write.metadata.metrics.column.amount'          = 'full',
  'write.metadata.metrics.column.description'     = 'none',
  'write.metadata.metrics.column.metadata_json'   = 'none'
);
```

Manifest-Größe selbstdiagnostisch prüfen — Faustregel: < 8 MB pro Manifest:

```sql
SELECT path, length/1024/1024 AS size_mb, added_data_files_count
FROM lake.gold.orders.manifests
ORDER BY length DESC LIMIT 10;
```

## 6. Format & Compression

```sql
ALTER TABLE lake.gold.orders SET TBLPROPERTIES (
  'format-version'                 = '2',          -- Pflicht (Position Deletes, Equality Deletes)
  'write.format.default'           = 'parquet',
  'write.parquet.compression-codec'= 'zstd',       -- besser als snappy für Cold Storage
  'write.parquet.compression-level'= '3',          -- 3 = guter Trade-off
  'write.distribution-mode'        = 'hash'         -- gleichmäßige Files pro Writer
);
```

| Codec | Compression-Ratio | CPU | Wann |
|---|---|---|---|
| `snappy` | mittel | niedrig | Read-heavy, viele kleine Queries |
| `zstd` | hoch | mittel | **Default für Lakehouse** — bestes Ratio bei OK-Decode |
| `gzip` | hoch | hoch | Archiv |
| `lz4` | mittel | sehr niedrig | Streaming-Writes mit hartem Latenz-Budget |

## 7. Schema Evolution — Regeln

| Operation | Iceberg-safe? | Hinweis |
|---|---|---|
| Spalte hinzufügen | ✅ ja | Default-Wert oder `NULL` für alte Files |
| Spalte umbenennen | ✅ ja (per Field-ID) | Reader müssen den Catalog kennen |
| Spalte löschen | ⚠️ vorsichtig | Alte Files behalten Daten — `DROP COLUMN` ist logisch, nicht physisch |
| Typ-Erweiterung (int→long, float→double, decimal precision↑) | ✅ ja | |
| Typ-Verengung | ❌ nein | Re-Write erforderlich |
| Spalten umordnen | ✅ ja | egal — Iceberg arbeitet per Field-ID |
| Required → Optional | ✅ ja | |
| Optional → Required | ❌ nein | nur wenn alle Daten non-null sind |

**Governance**: Schema-Changes gehen über die Schreib-Pipeline (Spark/Flink), nicht ad hoc per SQL. Sonst entsteht Drift zwischen Code und Tabelle.

## 8. Naming & Layering (Konvention für Lakekeeper)

```
warehouse: main
└── namespaces/
    ├── bronze.<source>          -- raw, schema-as-source
    │   └── orders_raw, events_raw
    ├── silver.<domain>          -- bereinigt, deduped, typisiert
    │   └── orders, customers
    ├── gold.<domain>            -- business-modelliert
    │   └── orders, dim_customer, fact_revenue
    └── serving.<consumer>       -- aggregiert für Endnutzer/Marts
        └── orders_daily, kpi_dashboard
```

Schreibrechte pro Namespace getrennt vergeben (Lakekeeper Permissions).

## 9. Beispiel: vollständige DDL für eine "kalte-performante" Tabelle

```sql
CREATE TABLE lake.gold.orders (
  order_id     bigint   NOT NULL,
  order_ts     timestamp NOT NULL,
  customer_id  bigint   NOT NULL,
  product_id   bigint,
  amount       decimal(12,2),
  status       string,
  channel      string,
  created_at   timestamp,
  updated_at   timestamp
)
USING iceberg
PARTITIONED BY (days(order_ts))
TBLPROPERTIES (
  'format-version'                                            = '2',
  'write.format.default'                                      = 'parquet',
  'write.parquet.compression-codec'                           = 'zstd',
  'write.parquet.compression-level'                           = '3',
  'write.target-file-size-bytes'                              = '536870912',
  'write.parquet.row-group-size-bytes'                        = '134217728',
  'write.distribution-mode'                                   = 'hash',
  'write.parquet.bloom-filter-enabled.column.customer_id'     = 'true',
  'write.parquet.bloom-filter-enabled.column.order_id'        = 'true',
  'write.metadata.metrics.default'                            = 'truncate(16)',
  'write.metadata.metrics.column.amount'                      = 'full',
  'commit.manifest.target-size-bytes'                         = '8388608',
  -- Maintenance-Konvention (siehe maintenance.md)
  'maintenance.compaction.enabled'                            = 'true',
  'maintenance.snapshot.expire_days'                          = '7',
  'maintenance.orphan.enabled'                                = 'true'
);

ALTER TABLE lake.gold.orders WRITE ORDERED BY (order_ts, customer_id);
```

## 10. Anti-Patterns

| Vermeiden | Warum |
|---|---|
| Partitionierung nach `customer_id` direkt | Tausende Partitionen → Manifest-Bloat |
| `STRING` für Datums-Spalten | Min/Max-Stats nutzlos, kein Pruning |
| Nullable-Schlüssel-Spalten | Bloom-Filter wirken nicht zuverlässig |
| Schreiben ohne `write.distribution-mode = hash` | Skewed Files, schlechte Compaction-Effizienz |
| Tausende Spalten in einer Tabelle | Metadata-Overhead, Stats-Cost. Lieber splitten oder JSON-Spalte |
| `DROP COLUMN` ohne Compaction | Alte Files behalten Daten — Storage-Bloat |
| `format-version=1` | V1 kann keine row-level deletes; CDC/Upsert geht nicht |
