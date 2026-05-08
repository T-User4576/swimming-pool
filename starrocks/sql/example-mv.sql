-- ============================================================================
-- Materialized View Beispiel
-- ============================================================================
-- Zweck: Sub-Second-Antworten auf wiederkehrende Aggregations-Queries.
-- Quelle: Iceberg-Mart in Lakekeeper-Catalog.
-- Refresh: Asynchron alle 15 Minuten.
-- Storage: liegt im StarRocks-StorageVolume (S3) -> kontrollierte Daten-Duplikation.
--
-- Governance: MV-DDL gehoert in den Argo-Workflow, der die Mart-Tabelle
-- erzeugt -- nicht ad hoc per Hand. Damit bleibt das Lebenszyklus-Modell
-- (Mart -> MV -> Refresh) konsistent.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_orders
COMMENT 'Tagesaggregat Orders pro Customer fuer Dashboard XYZ'
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
-- Query-Rewrite verifizieren: EXPLAIN sollte 'mv_daily_orders' zeigen,
-- nicht den Iceberg-Scan auf gold.orders.
-- ----------------------------------------------------------------------------
-- EXPLAIN SELECT customer_id, sum(revenue)
-- FROM lake.gold.orders
-- WHERE order_ts >= '2026-01-01'
-- GROUP BY customer_id;

-- ----------------------------------------------------------------------------
-- Manueller Refresh (z.B. fuer Initial-Load oder nach Schema-Change)
-- ----------------------------------------------------------------------------
-- REFRESH MATERIALIZED VIEW mv_daily_orders;
-- REFRESH MATERIALIZED VIEW mv_daily_orders PARTITION (day='2026-05-01') FORCE;

-- ----------------------------------------------------------------------------
-- Status / Health
-- ----------------------------------------------------------------------------
-- SHOW MATERIALIZED VIEWS WHERE NAME = 'mv_daily_orders';
-- SELECT * FROM information_schema.task_runs
-- WHERE task_name LIKE '%mv_daily_orders%' ORDER BY create_time DESC LIMIT 10;
