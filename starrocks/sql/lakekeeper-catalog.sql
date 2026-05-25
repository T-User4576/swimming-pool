-- ============================================================================
-- External Iceberg catalog against Lakekeeper (REST).
-- ============================================================================
-- Run once after cluster bootstrap, as cluster admin (root or equivalent).
-- Prerequisites:
--   - Lakekeeper is reachable at the configured URI.
--   - MinIO credentials are known to the CN (via Helm secret — see
--     starrocks/secrets/starrocks-s3-credentials.example.yaml).
--   - StarRocks version >= 3.3 (REST catalog stable).
--
-- Note: Auth/OIDC for Lakekeeper is covered in the separate /oidc plan.
-- Basic setup with a service token first (token via SealedSecret in
-- production, placeholder here).

CREATE EXTERNAL CATALOG IF NOT EXISTS lake
COMMENT 'Iceberg lakehouse via Lakekeeper'
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog",
  "iceberg.catalog.warehouse" = "main",
  -- "iceberg.catalog.oauth2-server-uri" = "<idp-token-endpoint>",
  -- "iceberg.catalog.credential" = "<client_id>:<client_secret>",

  -- S3 backend (MinIO in-cluster).
  "aws.s3.endpoint" = "http://minio.minio.svc.cluster.local:9000",
  "aws.s3.enable_path_style_access" = "true",
  "aws.s3.region" = "us-east-1",
  "aws.s3.access_key" = "<from-secret>",
  "aws.s3.secret_key" = "<from-secret>"
);

-- Verification.
SHOW CATALOGS;
SET CATALOG lake;
SHOW DATABASES;

-- Example query against an Iceberg table (adjust to your data).
-- SELECT count(*) FROM lake.gold.orders;
