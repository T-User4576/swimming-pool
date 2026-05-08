# SQL & Daten-Layout — Optimierungs-Cheatsheet

Kompakte Referenz für StarRocks (Shared-Data) auf Iceberg/Lakekeeper-Backend.

## 1. Tabellen-Layout

| Konzept | Wann | Syntax (StarRocks native / MV) | Hinweis |
|---|---|---|---|
| **Partitionierung** | Time-Series, häufiger Filter, hohe Cardinality | `PARTITION BY date_trunc('day', ts)` | Partition Pruning = Hebel #1, **immer** auf Filter-Spalte abstimmen |
| **Distribution / Bucketing** | Immer Pflicht | `DISTRIBUTED BY HASH(id) BUCKETS 32` | Buckets ≈ `CN-Anzahl × vCPU/4`; bei Skew anderen Key wählen |
| **Sort Key** (Primary Key Tables) | Range-Filter auf Spalte | `ORDER BY (event_ts, user_id)` | Korreliert Daten on-disk → Predicate Pushdown |
| **Replication** (shared-data) | Immer 1 | `"replication_num"="1"` | Storage liegt in S3, Multi-Replication wäre Verschwendung |

### Iceberg-Seite (für externe Catalog-Tabellen)

| Konzept | Wann | Syntax | Hinweis |
|---|---|---|---|
| **Hidden Partitioning** | Time-Series, Multi-Tenancy | `PARTITIONED BY (days(event_ts))` | Iceberg verwaltet Pfade, kein manuelles Spalten-Anlegen |
| **Sort Order** | häufig gefilterte Spalten | `WRITE ORDERED BY (col)` | File-Skipping via Min/Max-Stats |
| **Target File Size** | immer | `write.target-file-size-bytes=536870912` | 128–512 MB Sweet Spot |
| **Manifest Limit** | Watch-Metric | `rewriteManifests` regelmässig | < 1000 Manifests / Tabelle |
| **Snapshot Retention** | immer | `expireSnapshots` | < 100 Snapshots, sonst lahmes Planning |

## 2. Indizes (StarRocks native Tabellen / MVs)

| Index | Cardinality | Syntax | Kosten | Wann |
|---|---|---|---|---|
| **Bitmap** | < 100k unique | `CREATE INDEX ix ON t(status) USING BITMAP` | sehr klein | Status-/Enum-Spalten |
| **Bloom Filter** | hoch | `PROPERTIES("bloom_filter_columns"="user_id,order_id")` | mittel | Equality-Filter, nicht für Range |
| **N-Gram Bloom** | hoch | `INDEX ix(text) USING NGRAMBF(...)` | hoch (Speicher) | LIKE-Pattern-Suche |
| **ZoneMap** | implizit | — | frei | Range-Queries (immer aktiv) |

## 3. Statistiken (Cost-Based Optimizer)

| Zweck | Aktion | Wie oft |
|---|---|---|
| Default Auto-Collect | `enable_statistic_collect=true` in `fe.conf` | passiv, alle 30 min |
| Manuell nach Bulk-Load | `ANALYZE TABLE t WITH SYNC MODE` | nach Mart-Refresh |
| Full Stats wichtige Marts | `ANALYZE TABLE t WITH FULL SYNC MODE` | nightly |
| Histogramm bei Skew | `ANALYZE TABLE t UPDATE HISTOGRAM ON col` | bei schlechten Plänen |
| External (Iceberg) | `ANALYZE EXTERNAL TABLE lake.gold.x` | nach grossen Iceberg-Loads |

Verifikation: `SHOW STATS META WHERE TABLE_NAME='t'`.

## 4. Query-Schreibweise — Anti-Patterns

| Vermeiden | Stattdessen | Grund |
|---|---|---|
| `SELECT *` | Spalten explizit | Columnar = nur gelesene Spalten = weniger I/O |
| `WHERE date(ts) = '...'` | `WHERE ts >= ... AND ts < ...` | Funktion auf Spalte → kein Pushdown |
| `WHERE CAST(id AS STRING)=...` | korrekter Typ im Schema | Cast bricht Predicate Pushdown |
| `OR` über mehrere Spalten | `UNION ALL` mit getrennten Filtern | Optimizer wählt oft nur einen Index-Pfad |
| `IN (SELECT ...)` | `JOIN` oder `EXISTS` | Subquery-Rewrite nicht immer möglich |
| Implicit Cross Join | `JOIN ... ON` explizit | Verhindert Plan-Explosion |
| Filter erst im äusseren Select | Filter so früh wie möglich | Predicate Pushdown |
| `LIMIT` ohne `ORDER BY` deterministisch | `ORDER BY pk LIMIT n` | Sonst nicht-stabile Resultate |

## 5. JOIN-Tuning

| Hebel | Befehl | Wann |
|---|---|---|
| Broadcast für kleine Tabelle | `JOIN [BROADCAST] small ON ...` | Right-Side < 100 MB |
| Shuffle (Default) | `JOIN [SHUFFLE]` | grosse Faktentabelle × grosse Dim |
| Colocation | beide Tabellen `DISTRIBUTED BY HASH(same_key)` | wiederkehrender Join auf gleichem Key |
| Runtime Filter | `enable_global_runtime_filter=true` (default) | Default an, nicht abdrehen |
| JOIN Reorder | `cbo_enable_dp_join_reorder=true` | bei vielen Joins (>5) |

## 6. Materialized Views

| Pattern | MV-Typ | Refresh-Strategie |
|---|---|---|
| Tages-Aggregat | Async + Partitioned | `EVERY (INTERVAL 15 MINUTE)` oder Argo-getriggert |
| Hot-Subset (Filter) | Async mit WHERE | Event-driven nach Mart-Load |
| Join + Pre-Aggregation | Async, full refresh | nightly |
| Hot-Cache aus Iceberg | Async mit `storage_volume` | partition-based incremental |

Query-Rewrite verifizieren: `EXPLAIN <query>` muss MV-Namen zeigen, nicht Base-Tabelle.

## 7. Caching im Query-Pfad

| Cache | Wirkung | Steuerung |
|---|---|---|
| Datacache (Disk) | Block aus S3 → lokale NVMe | `cn.conf: datacache_enable=true` |
| Datacache (Mem) | Hottest Blöcke im RAM | `cn.conf: datacache_mem_size=20G` |
| Iceberg Metadata | Manifest/Schema | `enable_iceberg_metadata_cache=true` |
| Page Cache | In-Memory Block Index | `storage_page_cache_limit=20%` |
| Query Result Cache | identische Queries | `SET enable_query_cache=true` (Session/global) |
| Materialized View | pre-aggregiert | per MV-DDL |

Hit-Rate prüfen: `SET enable_profile=true; <query>; SHOW PROFILELIST;` → Datacache-Hit-% in Profile.

## 8. Performance-Debug-Workflow

| Schritt | Befehl | Was zeigt es |
|---|---|---|
| 1. Plan ansehen | `EXPLAIN <query>` | Logischer Plan, MV-Rewrite, Predicate Pushdown |
| 2. Costs prüfen | `EXPLAIN COSTS <query>` | CBO-Schätzungen — falsch = Stats stale |
| 3. Profile aktivieren | `SET enable_profile=true; <query>` | Echte Laufzeiten pro Operator |
| 4. Profile lesen | `SHOW PROFILELIST` → `ANALYZE PROFILE FROM '<id>'` | Bottleneck-Identifikation |
| 5. Slow Query Log | FE Audit Log (Loki) | Aggregation über viele Queries |

## 9. Faustregeln

- **Partition-Filter im WHERE muss zur Partition-Spec passen** — sonst Full Scan, Sub-Second tot.
- **Stats-Aktualität schlägt Index-Tuning** — ein veralteter Plan ist schlimmer als ein fehlender Index.
- **MV-Rewrite ist transparent, aber leise** — immer mit `EXPLAIN` verifizieren, sonst weiss niemand ob die MV wirkt.
- **Bei Sub-Second-SLA: warmer Cache eingeplant** — erste Query nach Pod-Restart ist immer langsam, ggf. Warmup-Query in CronWorkflow.
- **`SELECT *` aus Iceberg-Tabellen über StarRocks ist ein Sünden-Default** — kostet Netzwerk-Roundtrips zu S3 für irrelevante Spalten.
