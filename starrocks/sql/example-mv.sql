-- ============================================================================
-- Materialized view example.
-- ============================================================================
-- Purpose: sub-second answers to recurring aggregation queries.
-- Source: Iceberg mart in the Lakekeeper catalog.
-- Refresh: async every 15 minutes.
-- Storage: lives in the StarRocks storage volume (S3) -> controlled data
--          duplication.
--
-- Governance: MV DDL belongs in the Argo workflow that creates the mart
-- table — not ad hoc by hand. That keeps the lifecycle model
-- (mart -> MV -> refresh) consistent.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_orders
COMMENT 'Daily aggregate of orders per customer for dashboard XYZ'
DISTRIBUTED BY HASH(customer_id) BUCKETS 32
PARTITION BY (day)
REFRESH ASYNC EVERY (INTERVAL 15 MINUTE)
PROPERTIES (
  "replication_num" = "1",
  "storage_volume" = "builtin_storage_volume",
  "session.query_timeout" = "1800"
)
AS
SELECT
  date_trunc('day', order_ts) AS day,
  customer_id,
  count(*)        AS n_orders,
  sum(amount)     AS revenue,
  avg(amount)     AS avg_order_value
FROM lake.gold.orders
GROUP BY 1, 2;

-- ----------------------------------------------------------------------------
-- Verify query rewrite: EXPLAIN should show 'mv_daily_orders', not an
-- Iceberg scan on gold.orders.
-- ----------------------------------------------------------------------------
-- EXPLAIN SELECT customer_id, sum(revenue)
-- FROM lake.gold.orders
-- WHERE order_ts >= '2026-01-01'
-- GROUP BY customer_id;

-- ----------------------------------------------------------------------------
-- Manual refresh (e.g. for initial load or after a schema change).
-- ----------------------------------------------------------------------------
-- REFRESH MATERIALIZED VIEW mv_daily_orders;
-- REFRESH MATERIALIZED VIEW mv_daily_orders PARTITION (day='2026-05-01') FORCE;

-- ----------------------------------------------------------------------------
-- Status / health.
-- ----------------------------------------------------------------------------
-- SHOW MATERIALIZED VIEWS WHERE NAME = 'mv_daily_orders';
-- SELECT * FROM information_schema.task_runs
-- WHERE task_name LIKE '%mv_daily_orders%' ORDER BY create_time DESC LIMIT 10;
