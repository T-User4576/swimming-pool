-- ============================================================================
-- Materialized View Patterns
-- ============================================================================
-- Drei wiederkehrende Muster fuer den Lakehouse-Use-Case:
--   Pattern 1: Tages-Aggregat              -- vor-aggregierte Kennzahlen
--   Pattern 2: Hot-Subset (Filter)         -- nur "heisser" Zeitraum, ohne Aggregation
--   Pattern 3: Hot-Cache aus Iceberg       -- 1:1 Caching kleiner Dim-Tabellen
--
-- Konventionen (gelten fuer alle Patterns):
--   - storage_volume = builtin_storage_volume (Shared-Data, S3-backed)
--   - replication_num = 1 (Storage liegt in S3, Replikation waere Verschwendung)
--   - DDL gehoert in den Argo-Workflow der Mart-Pipeline (siehe argo/), nicht ad hoc
--   - Refresh-Strategie: scheduled bei stabilen Mustern, event-driven bei
--     unregelmaessigen Loads (Argo DAG ruft REFRESH MATERIALIZED VIEW auf)
-- ============================================================================


-- ============================================================================
-- Pattern 1: TAGES-AGGREGAT
-- ============================================================================
-- Use Case: Dashboard "Umsatz pro Kunde pro Tag", > 100 QPS, Sub-Second.
-- Quelle:   lake.gold.orders (Iceberg, partitioniert nach Tag)
-- Vorteil:  Query-Rewrite -- Optimizer ersetzt GROUP BY auf der Base-Tabelle
--           transparent durch Lesen der MV.
-- Refresh:  partition_refresh_number=7 -> nur die letzten 7 Tage refreshen,
--           Backfills fuer aeltere Partitionen explizit per FORCE.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_orders_daily
COMMENT 'Tagesaggregat Orders pro Customer'
PARTITION BY (day)
DISTRIBUTED BY HASH(customer_id) BUCKETS 32
REFRESH ASYNC EVERY (INTERVAL 15 MINUTE)
PROPERTIES (
  "replication_num" = "1",
  "storage_volume" = "builtin_storage_volume",
  "partition_refresh_number" = "7",
  "session.query_timeout" = "1800"
)
AS
SELECT
  date_trunc('day', order_ts)        AS day,
  customer_id,
  count(*)                           AS n_orders,
  sum(amount)                        AS revenue,
  avg(amount)                        AS avg_order_value,
  count(DISTINCT product_id)         AS distinct_products
FROM lake.gold.orders
GROUP BY 1, 2;

-- Verifikation Query-Rewrite:
-- EXPLAIN
-- SELECT customer_id, sum(revenue)
-- FROM lake.gold.orders
-- WHERE order_ts >= '2026-01-01'
-- GROUP BY customer_id;
-- Erwartung: Plan zeigt Scan auf 'mv_orders_daily', nicht auf 'lake.gold.orders'.


-- ============================================================================
-- Pattern 2: HOT-SUBSET (Filter)
-- ============================================================================
-- Use Case: Operatives Dashboard zeigt nur die letzten 90 Tage. Aeltere Daten
--           liegen in Iceberg und werden selten abgefragt -- es lohnt nicht,
--           sie in den Serving-Layer zu duplizieren.
-- Vorteil:  - Cache-Druck minimiert (Working-Set passt komplett in Datacache)
--           - partition_ttl_number droppt automatisch alte Partitionen
--           - Iceberg bleibt Source of Truth fuer Historie
-- Refresh:  Stuendlich; weil keine Aggregation, ist Refresh billig (CTAS-aehnlich).

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_orders_recent
COMMENT 'Hot-Subset: rolling 90-Tage Fenster der Orders'
PARTITION BY (date_trunc('day', order_ts))
DISTRIBUTED BY HASH(customer_id) BUCKETS 32
REFRESH ASYNC EVERY (INTERVAL 1 HOUR)
PROPERTIES (
  "replication_num" = "1",
  "storage_volume" = "builtin_storage_volume",
  "partition_refresh_number" = "1",
  "partition_ttl_number" = "90"
)
AS
SELECT
  order_ts,
  customer_id,
  product_id,
  amount,
  status,
  channel
FROM lake.gold.orders
WHERE order_ts >= date_sub(current_date(), 90);

-- Hinweis: Predicate-WHERE muss zur Partitionierung passen, sonst wird beim
-- Refresh nicht inkrementell gearbeitet, sondern voll gescanned.


-- ============================================================================
-- Pattern 3: HOT-CACHE AUS ICEBERG
-- ============================================================================
-- Use Case: Dim-Tabelle (lake.gold.dim_customer, ~5 GB) wird in praktisch jeder
--           Dashboard-Query gejoint. Iceberg-Latenz fuer den Lookup ist auch
--           mit Datacache spuerbar -> 1:1 Kopie als StarRocks-MV in S3
--           (Storage Volume), gleicher Catalog wie Marts.
-- Vorteil:  - Joins gegen native StarRocks-Tabelle, nicht External Catalog
--           - Bucketing/Distribution kompatibel zu Fakten-Tabellen
--             -> Colocation-Joins moeglich
--           - Index-Strategien (Bitmap/Bloom) auf der MV anwendbar (nach Aufbau)
-- Refresh:  Stuendlich; bei Slow-Changing-Dimension reicht das.
--           Bei TYP-2-SCD ggf. Trigger-basiert nach Mart-Load.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dim_customer_hot
COMMENT 'Hot-Cache 1:1 von lake.gold.dim_customer fuer schnelle Joins'
DISTRIBUTED BY HASH(customer_id) BUCKETS 16
REFRESH ASYNC EVERY (INTERVAL 1 HOUR)
PROPERTIES (
  "replication_num" = "1",
  "storage_volume" = "builtin_storage_volume",
  "session.query_timeout" = "1800"
)
AS
SELECT
  customer_id,
  customer_email,
  phone_number,
  country,
  segment,
  signup_date,
  status
FROM lake.gold.dim_customer;

-- Optional: Bitmap-Index fuer haeufige Status-Filter
-- ALTER MATERIALIZED VIEW mv_dim_customer_hot
--   ADD INDEX ix_status (status) USING BITMAP;

-- Optional: Colocation-Group fuer Joins mit Fakten-Tabellen
-- (beide Tabellen muessen identische Distribution-Keys + Bucket-Anzahl haben)


-- ============================================================================
-- Operative Befehle (alle drei Patterns)
-- ============================================================================

-- Status / Health
SHOW MATERIALIZED VIEWS WHERE NAME IN ('mv_orders_daily','mv_orders_recent','mv_dim_customer_hot');

-- Letzte Refresh-Runs
SELECT mv_name, state, error_message, last_refresh_start_time, last_refresh_finished_time
FROM information_schema.materialized_views
WHERE mv_name IN ('mv_orders_daily','mv_orders_recent','mv_dim_customer_hot');

-- Manueller Refresh (Argo nutzt diese Statements)
-- REFRESH MATERIALIZED VIEW mv_orders_daily;
-- REFRESH MATERIALIZED VIEW mv_orders_daily PARTITION ('2026-05-01') FORCE;
-- REFRESH MATERIALIZED VIEW mv_orders_recent FORCE;            -- voll
-- REFRESH MATERIALIZED VIEW mv_dim_customer_hot;
