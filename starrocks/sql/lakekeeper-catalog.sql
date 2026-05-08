-- ============================================================================
-- External Iceberg Catalog gegen Lakekeeper (REST)
-- ============================================================================
-- Ausfuehrung: einmalig nach Cluster-Bootstrap, als Cluster-Admin (root o.ae.)
-- Voraussetzungen:
--   - Lakekeeper laeuft erreichbar unter dem konfigurierten URI
--   - MinIO-Credentials sind dem CN bekannt (via Helm-Secret -- siehe
--     starrocks/secrets/starrocks-s3-credentials.example.yaml)
--   - StarRocks Version >= 3.3 (REST Catalog stabil)
--
-- Hinweis: Auth/OIDC fuer Lakekeeper kommt im separaten /oidc-Plan.
-- Hier zunaechst Basic-Setup mit Service-Token (Token via SealedSecret in
-- Production, hier Platzhalter).

CREATE EXTERNAL CATALOG IF NOT EXISTS lake
COMMENT 'Iceberg Lakehouse via Lakekeeper'
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog",
  "iceberg.catalog.warehouse" = "main",
  -- "iceberg.catalog.oauth2-server-uri" = "<idp-token-endpoint>",
  -- "iceberg.catalog.credential" = "<client_id>:<client_secret>",

  -- S3 Backend (MinIO im Cluster)
  "aws.s3.endpoint" = "http://minio.minio.svc.cluster.local:9000",
  "aws.s3.enable_path_style_access" = "true",
  "aws.s3.region" = "us-east-1",
  "aws.s3.access_key" = "<from-secret>",
  "aws.s3.secret_key" = "<from-secret>"
);

-- Verifikation
SHOW CATALOGS;
SET CATALOG lake;
SHOW DATABASES;

-- Beispiel-Query gegen Iceberg-Tabelle (anpassen)
-- SELECT count(*) FROM lake.gold.orders;
