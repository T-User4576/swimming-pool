-- ============================================================================
-- Materialized view patterns.
-- ============================================================================
-- Three recurring patterns for the lakehouse use case:
--   Pattern 1: daily aggregate          -- pre-aggregated metrics
--   Pattern 2: hot subset (filter)      -- only the "hot" window, no aggregation
--   Pattern 3: hot cache from Iceberg   -- 1:1 caching of small dimension tables
--
-- Conventions (apply to all patterns):
--   - storage_volume = builtin_storage_volume (shared-data, S3-backed).
--   - replication_num = 1 (storage lives in S3; replication would be waste).
--   - DDL belongs in the Argo workflow of the mart pipeline (see argo/),
--     not done ad hoc.
--   - Refresh strategy: scheduled for stable patterns, event-driven for
--     irregular loads (Argo DAG calls REFRESH MATERIALIZED VIEW).
-- ============================================================================


-- ============================================================================
-- Pattern 1: DAILY AGGREGATE.
-- ============================================================================
-- Use case: dashboard "revenue per customer per day", > 100 QPS, sub-second.
-- Source:   lake.gold.orders (Iceberg, partitioned by day).
-- Benefit:  query rewrite — the optimizer transparently rewrites GROUP BY
--           on the base table to a read against the MV.
-- Refresh:  partition_refresh_number=7 -> only the last 7 days, backfills
--           for older partitions via explicit FORCE.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_orders_daily
COMMENT 'Daily aggregate of orders per customer'
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

-- Verify query rewrite:
-- EXPLAIN
-- SELECT customer_id, sum(revenue)
-- FROM lake.gold.orders
-- WHERE order_ts >= '2026-01-01'
-- GROUP BY customer_id;
-- Expectation: plan scans 'mv_orders_daily', not 'lake.gold.orders'.


-- ============================================================================
-- Pattern 2: HOT SUBSET (FILTER).
-- ============================================================================
-- Use case: operational dashboard shows only the last 90 days. Older data
--           lives in Iceberg and is queried rarely — not worth duplicating
--           into the serving layer.
-- Benefit:  - cache pressure minimized (working set fits the datacache).
--           - partition_ttl_number drops old partitions automatically.
--           - Iceberg remains source of truth for history.
-- Refresh:  hourly; with no aggregation, refresh is cheap (CTAS-like).

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_orders_recent
COMMENT 'Hot subset: rolling 90-day window of orders'
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

-- Note: the WHERE predicate must align with partitioning, otherwise the
-- refresh is a full scan instead of incremental.


-- ============================================================================
-- Pattern 3: HOT CACHE FROM ICEBERG.
-- ============================================================================
-- Use case: dimension table (lake.gold.dim_customer, ~5 GB) is joined in
--           practically every dashboard query. Iceberg lookup latency is
--           noticeable even with datacache -> 1:1 copy as a StarRocks MV
--           in S3 (storage volume), same catalog as the marts.
-- Benefit:  - joins against native StarRocks table, not an external catalog.
--           - bucketing/distribution compatible with fact tables ->
--             colocation joins are possible.
--           - index strategies (bitmap/bloom) usable on the MV (after build).
-- Refresh:  hourly; with slow-changing-dimensions that is enough.
--           For type-2 SCD, trigger-based after mart load may be better.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dim_customer_hot
COMMENT 'Hot cache 1:1 of lake.gold.dim_customer for fast joins'
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

-- Optional: bitmap index for common status filters.
-- ALTER MATERIALIZED VIEW mv_dim_customer_hot
--   ADD INDEX ix_status (status) USING BITMAP;

-- Optional: colocation group for joins with fact tables (both tables
-- must share identical distribution keys + bucket counts).


-- ============================================================================
-- Operational commands (all three patterns).
-- ============================================================================

-- Status / health.
SHOW MATERIALIZED VIEWS WHERE NAME IN ('mv_orders_daily','mv_orders_recent','mv_dim_customer_hot');

-- Last refresh runs.
SELECT mv_name, state, error_message, last_refresh_start_time, last_refresh_finished_time
FROM information_schema.materialized_views
WHERE mv_name IN ('mv_orders_daily','mv_orders_recent','mv_dim_customer_hot');

-- Manual refresh (Argo uses these statements).
-- REFRESH MATERIALIZED VIEW mv_orders_daily;
-- REFRESH MATERIALIZED VIEW mv_orders_daily PARTITION ('2026-05-01') FORCE;
-- REFRESH MATERIALIZED VIEW mv_orders_recent FORCE;            -- full
-- REFRESH MATERIALIZED VIEW mv_dim_customer_hot;
