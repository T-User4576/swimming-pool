# =============================================================================
# Transform spec runner — generic, declarative PySpark job.
# =============================================================================
# Invoked by the Argo WorkflowTemplate 'spark-transform'. Reads a
# declarative pipeline spec (YAML) and executes it:
#
#   read source -> optionally validate/enforce schema
#   -> apply steps sequentially -> write sink
#
# The spec schema is documented in ../transform-spec.md. Source, step and
# sink each go through a registry (SOURCES / STEPS / SINKS) and are
# extensible — a new type = one handler + one registry entry, no main()
# surgery. Same agnostic / registry pattern as in
# iceberg/spark/maintenance.py.
#
# Invocation via spark-submit:
#   spark-submit runner.py \
#       --catalog lake \
#       --spec /opt/spark/specs/gold-revenue-daily.yaml \
#       [--dry-run]
#
# Dependencies:
#   - PyYAML must be available in the Spark image (see ../README.md,
#     section "Code-Distribution").
#   - The kafka source type additionally needs the package
#     org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 (set as
#     deps.packages in the Argo workflow).
# =============================================================================

import argparse
import logging
import sys
import time

import yaml

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("transform-runner")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Transform spec runner")
    p.add_argument("--spec", required=True, help="Path to the pipeline spec YAML")
    p.add_argument("--catalog", required=True, help="Iceberg catalog name (Spark side, e.g. 'lake')")
    p.add_argument("--dry-run", action="store_true", help="No write; instead, explain + count")
    return p.parse_args()


def get_session(name: str) -> SparkSession:
    # Catalog config (spark.sql.catalog.<lake>.*) is set via spark-submit
    # --conf — see argo/transform-workflow.yaml. Same as in maintenance.py.
    return (
        SparkSession.builder.appName(f"transform-{name}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


# -----------------------------------------------------------------------------
# Load + hard-validate the spec (fail-fast instead of a test suite).
# -----------------------------------------------------------------------------
def load_spec(path: str) -> dict:
    with open(path) as fh:
        spec = yaml.safe_load(fh)
    if not isinstance(spec, dict):
        raise ValueError(f"Spec {path} is not a YAML mapping")

    for field in ("name", "source", "sink"):
        if field not in spec:
            raise ValueError(f"Spec: required field '{field}' missing")

    src_type = spec["source"].get("type")
    if src_type not in SOURCES:
        raise ValueError(f"Spec: unknown source.type '{src_type}', allowed: {sorted(SOURCES)}")

    sink_type = spec["sink"].get("type")
    if sink_type not in SINKS:
        raise ValueError(f"Spec: unknown sink.type '{sink_type}', allowed: {sorted(SINKS)}")

    sink_mode = spec["sink"].get("mode", "append")
    if sink_mode not in ("append", "overwrite_partitions", "merge"):
        raise ValueError(f"Spec: unknown sink.mode '{sink_mode}'")
    if sink_mode == "merge" and not spec["sink"].get("merge_keys"):
        raise ValueError("Spec: sink.mode=merge requires 'merge_keys'")

    steps = spec.get("steps") or []
    if not isinstance(steps, list):
        raise ValueError("Spec: 'steps' must be a list")
    for i, step in enumerate(steps):
        st = step.get("type")
        if st not in STEPS:
            raise ValueError(f"Spec: step #{i} unknown type '{st}', allowed: {sorted(STEPS)}")
        if st == "aggregate":
            for a in step.get("aggregations", []):
                if a.get("func") not in AGG_FUNCS:
                    raise ValueError(
                        f"Spec: step #{i} unknown agg function '{a.get('func')}', "
                        f"allowed: {sorted(AGG_FUNCS)}")

    schema = spec.get("schema")
    if schema is not None:
        if schema.get("mode") not in ("validate", "enforce"):
            raise ValueError("Spec: schema.mode must be 'validate' or 'enforce'")
        if not schema.get("columns"):
            raise ValueError("Spec: schema.columns missing or empty")
    if src_type == "kafka" and not schema:
        raise ValueError("Spec: kafka source requires a 'schema' block (value deserialization)")

    return spec


# -----------------------------------------------------------------------------
# Schema helpers
# -----------------------------------------------------------------------------
# Spark DDL types are the reference; a few common aliases are normalized.
_TYPE_ALIASES = {
    "long": "bigint", "int": "integer", "short": "smallint",
    "byte": "tinyint", "dec": "decimal", "numeric": "decimal", "real": "float",
}


def _norm_type(t: str) -> str:
    t = str(t).lower().replace(" ", "")
    return _TYPE_ALIASES.get(t, t)


def build_ddl(columns: list[dict]) -> str:
    """Builds a Spark DDL schema string from schema.columns (without NOT NULL
    — nullability is not reliably enforceable on the read side)."""
    return ", ".join(f"`{c['name']}` {c['type']}" for c in columns)


def apply_schema(df: DataFrame, schema: dict) -> DataFrame:
    """mode=validate: checks columns/types, raises on mismatch.
    mode=enforce: select + cast to the target schema."""
    mode = schema["mode"]
    columns = schema["columns"]
    actual = {f.name: f for f in df.schema.fields}

    if mode == "validate":
        for c in columns:
            name = c["name"]
            if name not in actual:
                raise ValueError(f"Schema validation: column '{name}' missing in source")
            want = _norm_type(c["type"])
            got = _norm_type(actual[name].dataType.simpleString())
            if want != got:
                raise ValueError(
                    f"Schema validation: column '{name}' has type '{got}', expected '{want}'")
            if c.get("nullable") is False and actual[name].nullable:
                log.warning("Schema validation: column '%s' is nullable, expected NOT NULL", name)
        if schema.get("on_extra_columns", "ignore") == "error":
            extra = sorted(set(actual) - {c["name"] for c in columns})
            if extra:
                raise ValueError(f"Schema validation: unexpected columns {extra}")
        return df

    # mode == "enforce"
    on_missing = schema.get("on_missing_columns", "error")
    select_cols = []
    for c in columns:
        name = c["name"]
        if name not in actual:
            if on_missing == "error":
                raise ValueError(f"Schema enforcement: column '{name}' missing in source")
            select_cols.append(F.lit(None).cast(c["type"]).alias(name))
            continue
        select_cols.append(F.col(name).cast(c["type"]).alias(name))
    return df.select(*select_cols)


# -----------------------------------------------------------------------------
# Source handlers — registry SOURCES
# -----------------------------------------------------------------------------
def read_iceberg(spark: SparkSession, source: dict, schema: dict | None) -> DataFrame:
    table = source["table"]
    snap = source.get("snapshot")
    if snap is not None:
        log.info("[source] iceberg %s @ snapshot %s", table, snap)
        return spark.read.option("snapshot-id", str(snap)).format("iceberg").load(table)
    log.info("[source] iceberg %s", table)
    return spark.table(table)


def read_file(spark: SparkSession, source: dict, schema: dict | None) -> DataFrame:
    path = source["path"]
    fmt = source["format"]
    opts = {k: str(v) for k, v in (source.get("options") or {}).items()}
    log.info("[source] file %s format=%s", path, fmt)
    reader = spark.read.format(fmt).options(**opts)
    # Read-side schema only for text formats; parquet/orc carry their own.
    if schema and fmt in ("csv", "json"):
        reader = reader.schema(build_ddl(schema["columns"]))
    # Compression (.gz/.bz2/.zstd/...) is detected by Spark from the extension.
    return reader.load(path)


def read_kafka(spark: SparkSession, source: dict, schema: dict | None) -> DataFrame:
    # Deliberately a BOUNDED batch read over a fixed offset range — not a
    # continuous consumer. Streaming Kafka -> Iceberg stays Flink's role
    # (AGENTS.md §2).
    servers = source["bootstrap_servers"]
    topic = source["topic"]
    vfmt = source.get("value_format", "json")
    log.info("[source] kafka topic=%s servers=%s value_format=%s", topic, servers, vfmt)

    reader = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", topic)
        .option("startingOffsets", source.get("starting_offsets", "earliest"))
        .option("endingOffsets", source.get("ending_offsets", "latest"))
    )
    for k, v in (source.get("options") or {}).items():
        reader = reader.option(k, str(v))
    raw = reader.load()

    ddl = build_ddl(schema["columns"])
    if vfmt == "json":
        return raw.select(
            F.from_json(F.col("value").cast("string"), ddl).alias("_v")
        ).select("_v.*")
    if vfmt == "avro":
        # from_avro expects an Avro JSON schema (not Spark DDL) and the
        # spark-avro package — not covered in v1, see transform-spec.md.
        raise NotImplementedError(
            "value_format=avro is not implemented in v1 — use value_format=json")
    raise ValueError(f"Unknown value_format: {vfmt}")


# -----------------------------------------------------------------------------
# Step handlers — registry STEPS. Uniform signature (spark, df, step).
# -----------------------------------------------------------------------------
def step_rename(spark: SparkSession, df: DataFrame, step: dict) -> DataFrame:
    for old, new in step["columns"].items():
        df = df.withColumnRenamed(old, new)
    return df


def step_select(spark: SparkSession, df: DataFrame, step: dict) -> DataFrame:
    return df.select(*step["columns"])


def step_cast(spark: SparkSession, df: DataFrame, step: dict) -> DataFrame:
    for col, typ in step["columns"].items():
        df = df.withColumn(col, F.col(col).cast(typ))
    return df


def step_filter(spark: SparkSession, df: DataFrame, step: dict) -> DataFrame:
    return df.where(F.expr(step["where"]))


def step_derive(spark: SparkSession, df: DataFrame, step: dict) -> DataFrame:
    for name, expr in step["columns"].items():
        df = df.withColumn(name, F.expr(expr))
    return df


def step_dedup(spark: SparkSession, df: DataFrame, step: dict) -> DataFrame:
    keys = step["keys"]
    order_by = step.get("order_by")
    if not order_by:
        # Without order_by: arbitrary row per key — cheaper, no window sort.
        return df.dropDuplicates(keys)
    # With order_by: deterministically keep the "first" row per key.
    order_cols = []
    for o in order_by:
        c = F.col(o["column"])
        order_cols.append(c.desc() if str(o.get("dir", "asc")).lower() == "desc" else c.asc())
    w = Window.partitionBy(*keys).orderBy(*order_cols)
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def step_aggregate(spark: SparkSession, df: DataFrame, step: dict) -> DataFrame:
    group_by = [F.expr(g) for g in step["group_by"]]
    aggs = [
        AGG_FUNCS[a["func"]](F.col(a["column"])).alias(a["as"])
        for a in step["aggregations"]
    ]
    return df.groupBy(*group_by).agg(*aggs)


def step_sql(spark: SparkSession, df: DataFrame, step: dict) -> DataFrame:
    # Escape hatch: free Spark SQL against the current intermediate (_in).
    df.createOrReplaceTempView("_in")
    return spark.sql(step["query"])


# -----------------------------------------------------------------------------
# Sink handlers — registry SINKS
# -----------------------------------------------------------------------------
def write_iceberg(spark: SparkSession, df: DataFrame, sink: dict, dry: bool) -> None:
    table = sink["table"]
    mode = sink.get("mode", "append")

    if dry:
        log.info("[sink %s] DRY-RUN mode=%s — no write, no snapshot", table, mode)
        df.explain(mode="formatted")
        log.info("[sink %s] DRY-RUN result schema:", table)
        df.printSchema()
        # count() here is intentional (sanity check) — NOT on the prod path.
        log.info("[sink %s] DRY-RUN result rows: %d", table, df.count())
        return

    if not spark.catalog.tableExists(table):
        if sink.get("create_if_not_exists", False):
            if sink.get("partitioned_by"):
                raise ValueError(
                    f"create_if_not_exists with partitioned_by is not supported "
                    f"in v1 — create table {table} up front via DDL "
                    f"(see iceberg/table-design.md)")
            log.info("[sink %s] table missing — creating it", table)
            df.writeTo(table).create()
            return
        raise ValueError(
            f"Sink table {table} does not exist. Create it up front via DDL "
            f"(see iceberg/table-design.md) or set sink.create_if_not_exists.")

    writer = df.writeTo(table)
    for k, v in (sink.get("write_options") or {}).items():
        writer = writer.option(k, str(v))

    if mode == "append":
        writer.append()
    elif mode == "overwrite_partitions":
        writer.overwritePartitions()
    elif mode == "merge":
        keys = sink["merge_keys"]
        df.createOrReplaceTempView("_src")
        cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
        spark.sql(
            f"MERGE INTO {table} t USING _src s ON {cond} "
            f"WHEN MATCHED THEN UPDATE SET * "
            f"WHEN NOT MATCHED THEN INSERT *")
    else:
        raise ValueError(f"Unknown sink.mode: {mode}")
    log.info("[sink %s] written (mode=%s)", table, mode)


# -----------------------------------------------------------------------------
# Registries — new type = one handler + one entry here.
# -----------------------------------------------------------------------------
SOURCES = {
    "iceberg": read_iceberg,
    "file": read_file,
    "kafka": read_kafka,
}

STEPS = {
    "rename": step_rename,
    "select": step_select,
    "cast": step_cast,
    "filter": step_filter,
    "derive": step_derive,
    "dedup": step_dedup,
    "aggregate": step_aggregate,
    "sql": step_sql,
}

SINKS = {
    "iceberg": write_iceberg,
}

AGG_FUNCS = {
    "sum": F.sum,
    "count": F.count,
    "count_distinct": F.countDistinct,
    "avg": F.avg,
    "min": F.min,
    "max": F.max,
    "first": F.first,
    "last": F.last,
}


# -----------------------------------------------------------------------------
# Orchestration
# -----------------------------------------------------------------------------
def main() -> int:
    args = parse_args()
    spec = load_spec(args.spec)   # fail-fast: validates the whole spec up front

    log.info("=== Pipeline '%s'%s ===", spec["name"], " (DRY-RUN)" if args.dry_run else "")
    if spec.get("description"):
        log.info("%s", str(spec["description"]).strip())

    spark = get_session(spec["name"])
    try:
        spark.sql(f"USE {args.catalog}")

        schema = spec.get("schema")

        # 1) Source
        source = spec["source"]
        df = SOURCES[source["type"]](spark, source, schema)

        # 2) Validate / enforce schema
        if schema:
            df = apply_schema(df, schema)
            log.info("Schema %s applied (%d columns)", schema["mode"], len(schema["columns"]))

        # 3) Steps sequentially
        for i, step in enumerate(spec.get("steps") or []):
            start = time.monotonic()
            df = STEPS[step["type"]](spark, df, step)
            log.info("Step #%d %s — %.1fs", i, step["type"], time.monotonic() - start)

        # 4) Sink
        sink = spec["sink"]
        SINKS[sink["type"]](spark, df, sink, args.dry_run)

        log.info("Pipeline '%s' finished", spec["name"])
        return 0
    except Exception as e:
        log.exception("Pipeline '%s' failed: %s", spec["name"], e)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
