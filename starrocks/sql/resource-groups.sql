-- ============================================================================
-- Resource Groups fuer Workload-Isolation
-- ============================================================================
-- Ziel: BI-Heavy-Queries oder ETL-Jobs duerfen keine API-Latenzen toeten.
-- Anwendung: Bei >500 parallelen Usern und gemischtem Workload Pflicht.
--
-- Regel-Matching: Reihenfolge der Properties (TO-Klausel) bestimmt Zuordnung.
-- Erste passende Regel gewinnt.

-- ----------------------------------------------------------------------------
-- API-Workload (Customer-facing Dashboards, niedrige Latenz)
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
-- BI-Workload (interne Analysten, laengere Queries OK)
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
-- ETL/MV-Refresh (Spark/Argo-getrieben, asynchron)
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
-- Verifikation
-- ----------------------------------------------------------------------------
SHOW RESOURCE GROUPS ALL;

-- Active Resource Group der eigenen Session zeigen
-- SELECT current_resource_group();
