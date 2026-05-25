# =============================================================================
# Iceberg maintenance — agnostic PySpark job
# =============================================================================
# Invoked by the Argo CronWorkflow. Lists all Iceberg tables in the Lakekeeper
# catalog, reads the per-table maintenance properties (convention: see
# ../maintenance.md section 9) and calls the matching Spark procedures.
#
# Properties schema (all optional, with defaults):
#   maintenance.compaction.enabled              [true]
#   maintenance.compaction.window_days          [7]
#   maintenance.compaction.target_file_size_bytes [536870912]
#   maintenance.snapshot.enabled                [true]
#   maintenance.snapshot.retain_days            [7]
#   maintenance.snapshot.retain_last            [5]
#   maintenance.orphan.enabled                  [true]
#   maintenance.orphan.older_than_days          [7]
#   maintenance.manifest.enabled                [true]
#   maintenance.position_deletes.enabled        [false]
#
# Invocation via spark-submit:
#   spark-submit maintenance.py \
#       --catalog lake \
#       --jobs compaction,snapshot,orphan,manifest \
#       [--namespaces gold,silver]   # optional filter
#       [--dry-run]
# =============================================================================

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Iterable

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("iceberg-maintenance")

DEFAULTS = {
    "maintenance.compaction.enabled": "true",
    "maintenance.compaction.window_days": "7",
    "maintenance.compaction.target_file_size_bytes": "536870912",
    "maintenance.snapshot.enabled": "true",
    "maintenance.snapshot.retain_days": "7",
    "maintenance.snapshot.retain_last": "5",
    "maintenance.orphan.enabled": "true",
    "maintenance.orphan.older_than_days": "7",
    "maintenance.manifest.enabled": "true",
    "maintenance.position_deletes.enabled": "false",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True, help="Iceberg catalog name (Spark side, e.g. 'lake')")
    p.add_argument(
        "--jobs",
        default="compaction,snapshot,orphan,manifest",
        help="Comma-separated list: compaction,snapshot,orphan,manifest,position_deletes",
    )
    p.add_argument("--namespaces", default="", help="Comma-separated namespace whitelist")
    p.add_argument("--tables", default="", help="Comma-separated table whitelist (db.table)")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def get_session(catalog: str) -> SparkSession:
    # Catalog config is set via spark-submit --conf (see Argo workflow).
    return (
        SparkSession.builder.appName(f"iceberg-maintenance-{catalog}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def list_tables(spark: SparkSession, catalog: str, namespaces_filter: list[str], tables_filter: list[str]) -> Iterable[tuple[str, str]]:
    """Yields (namespace, table) tuples discovered via the Iceberg catalog."""
    if tables_filter:
        for t in tables_filter:
            ns, name = t.split(".", 1)
            yield ns, name
        return

    namespaces = [r[0] for r in spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()]
    for ns in namespaces:
        if namespaces_filter and ns not in namespaces_filter:
            continue
        for r in spark.sql(f"SHOW TABLES IN {catalog}.{ns}").collect():
            yield ns, r[1]   # row schema: namespace, tableName, isTemporary


def get_props(spark: SparkSession, fqn: str) -> dict[str, str]:
    """Reads TBLPROPERTIES, merges with DEFAULTS."""
    rows = spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()
    props = {r[0]: r[1] for r in rows}
    return {**DEFAULTS, **props}


def is_enabled(props: dict, key: str) -> bool:
    return str(props.get(key, "false")).lower() == "true"


def run_compaction(spark: SparkSession, fqn: str, props: dict, dry: bool) -> None:
    if not is_enabled(props, "maintenance.compaction.enabled"):
        log.info("[%s] compaction skipped (disabled)", fqn)
        return
    window_days = int(props["maintenance.compaction.window_days"])
    target_bytes = props["maintenance.compaction.target_file_size_bytes"]
    cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).strftime("%Y-%m-%d %H:%M:%S")

    sort_order = props.get("write.sort-order")  # if set in TBLPROPERTIES
    sort_clause = f", sort_order => '{sort_order}'" if sort_order else ""
    strategy = "sort" if sort_order else "binpack"

    sql = f"""
        CALL system.rewrite_data_files(
          table => '{fqn}',
          strategy => '{strategy}'
          {sort_clause},
          where => "_PARTITION_TIME >= TIMESTAMP '{cutoff}'",
          options => map(
            'target-file-size-bytes', '{target_bytes}',
            'min-input-files', '5',
            'partial-progress.enabled', 'true',
            'rewrite-job-order', 'bytes-desc'
          )
        )
    """
    # Note: the WHERE above is illustrative. In practice the filter column
    # must be the table's partition column. That can be derived from
    # SHOW PARTITIONS / system.partitions — the real workflow should
    # build this dynamically per table.
    log.info("[%s] compaction: strategy=%s window=%dd target=%sB", fqn, strategy, window_days, target_bytes)
    if dry:
        log.info("[%s] DRY-RUN, would execute:\n%s", fqn, sql)
        return
    spark.sql(sql)


def run_snapshot_expiry(spark: SparkSession, fqn: str, props: dict, dry: bool) -> None:
    if not is_enabled(props, "maintenance.snapshot.enabled"):
        log.info("[%s] snapshot expiry skipped (disabled)", fqn)
        return
    retain_days = int(props["maintenance.snapshot.retain_days"])
    retain_last = int(props["maintenance.snapshot.retain_last"])
    older_than = (datetime.now(timezone.utc) - timedelta(days=retain_days)).strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""
        CALL system.expire_snapshots(
          table => '{fqn}',
          older_than => TIMESTAMP '{older_than}',
          retain_last => {retain_last},
          max_concurrent_deletes => 100
        )
    """
    log.info("[%s] expire_snapshots older_than=%s retain_last=%d", fqn, older_than, retain_last)
    if dry:
        log.info("[%s] DRY-RUN: %s", fqn, sql)
        return
    spark.sql(sql)


def run_orphan(spark: SparkSession, fqn: str, props: dict, dry: bool) -> None:
    if not is_enabled(props, "maintenance.orphan.enabled"):
        log.info("[%s] orphan cleanup skipped (disabled)", fqn)
        return
    days = int(props["maintenance.orphan.older_than_days"])
    older_than = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""
        CALL system.remove_orphan_files(
          table => '{fqn}',
          older_than => TIMESTAMP '{older_than}',
          dry_run => {str(dry).lower()},
          max_concurrent_deletes => 50
        )
    """
    log.info("[%s] remove_orphan_files older_than=%s dry=%s", fqn, older_than, dry)
    spark.sql(sql)   # remove_orphan_files supports dry_run natively


def run_manifest_rewrite(spark: SparkSession, fqn: str, props: dict, dry: bool) -> None:
    if not is_enabled(props, "maintenance.manifest.enabled"):
        log.info("[%s] manifest rewrite skipped (disabled)", fqn)
        return
    sql = f"CALL system.rewrite_manifests('{fqn}')"
    log.info("[%s] rewrite_manifests", fqn)
    if dry:
        log.info("[%s] DRY-RUN: %s", fqn, sql)
        return
    spark.sql(sql)


def run_position_deletes(spark: SparkSession, fqn: str, props: dict, dry: bool) -> None:
    if not is_enabled(props, "maintenance.position_deletes.enabled"):
        return
    sql = f"""
        CALL system.rewrite_position_delete_files(
          table => '{fqn}',
          options => map('rewrite-all', 'true')
        )
    """
    log.info("[%s] rewrite_position_delete_files", fqn)
    if dry:
        log.info("[%s] DRY-RUN: %s", fqn, sql)
        return
    spark.sql(sql)


JOBS = {
    "compaction": run_compaction,
    "snapshot": run_snapshot_expiry,
    "orphan": run_orphan,
    "manifest": run_manifest_rewrite,
    "position_deletes": run_position_deletes,
}


def main() -> int:
    args = parse_args()
    spark = get_session(args.catalog)
    spark.sql(f"USE {args.catalog}")

    selected = [j.strip() for j in args.jobs.split(",") if j.strip()]
    ns_filter = [n.strip() for n in args.namespaces.split(",") if n.strip()]
    tbl_filter = [t.strip() for t in args.tables.split(",") if t.strip()]

    failures = 0
    for ns, name in list_tables(spark, args.catalog, ns_filter, tbl_filter):
        fqn = f"{args.catalog}.{ns}.{name}"
        log.info("=== %s ===", fqn)
        try:
            props = get_props(spark, fqn)
            for job in selected:
                if job not in JOBS:
                    log.warning("unknown job %s, skipping", job)
                    continue
                start = time.monotonic()
                JOBS[job](spark, fqn, props, args.dry_run)
                log.info("[%s] %s done in %.1fs", fqn, job, time.monotonic() - start)
        except Exception as e:
            failures += 1
            log.exception("[%s] failed: %s", fqn, e)

    spark.stop()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
