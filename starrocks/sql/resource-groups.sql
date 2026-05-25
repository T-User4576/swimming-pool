-- ============================================================================
-- Resource groups for workload isolation.
-- ============================================================================
-- Goal: BI heavy queries and ETL jobs must not kill API latencies.
-- When to use: mandatory at >500 parallel users with mixed workload.
--
-- Rule matching: property order (TO clause) decides assignment.
-- First matching rule wins.

-- ----------------------------------------------------------------------------
-- API workload (customer-facing dashboards, low latency).
-- ----------------------------------------------------------------------------
CREATE RESOURCE GROUP rg_api
TO (user='svc_api')
WITH (
  'type' = 'normal',
  'cpu_core_limit' = '20',
  'mem_limit' = '40%',
  'concurrency_limit' = '200',
  'big_query_cpu_second_limit' = '10',
  'big_query_scan_rows_limit' = '100000000',
  'big_query_mem_limit' = '10737418240'
);

-- ----------------------------------------------------------------------------
-- BI workload (internal analysts, longer queries OK).
-- ----------------------------------------------------------------------------
CREATE RESOURCE GROUP rg_bi
TO (user='bi_team')
WITH (
  'type' = 'normal',
  'cpu_core_limit' = '40',
  'mem_limit' = '40%',
  'concurrency_limit' = '50'
);

-- ----------------------------------------------------------------------------
-- ETL / MV refresh (Spark/Argo driven, asynchronous).
-- ----------------------------------------------------------------------------
CREATE RESOURCE GROUP rg_etl
TO (user='svc_etl')
WITH (
  'type' = 'normal',
  'cpu_core_limit' = '60',
  'mem_limit' = '60%',
  'concurrency_limit' = '10'
);

-- ----------------------------------------------------------------------------
-- Verification.
-- ----------------------------------------------------------------------------
SHOW RESOURCE GROUPS ALL;

-- Show the active resource group of the current session.
-- SELECT current_resource_group();
