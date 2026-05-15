# =============================================================================
# Transform-Spec-Runner — generischer, deklarativer PySpark-Job
# =============================================================================
# Wird vom Argo WorkflowTemplate 'spark-transform' aufgerufen. Liest eine
# deklarative Pipeline-Spec (YAML) und führt sie aus:
#
#   Source lesen -> optional Schema validieren/erzwingen
#   -> Steps sequentiell anwenden -> Sink schreiben
#
# Das Spec-Schema ist in ../transform-spec.md dokumentiert. Source, Step und
# Sink sind je über eine Registry (SOURCES / STEPS / SINKS) erweiterbar — ein
# neuer Typ = ein Handler + ein Registry-Eintrag, ohne Eingriff in main().
# Dasselbe Agnostik-/Registry-Prinzip wie in iceberg/spark/maintenance.py.
#
# Aufruf via spark-submit:
#   spark-submit runner.py \
#       --catalog lake \
#       --spec /opt/spark/specs/gold-revenue-daily.yaml \
#       [--dry-run]
#
# Abhängigkeiten:
#   - PyYAML muss im Spark-Image vorhanden sein (siehe ../README.md,
#     Abschnitt "Code-Distribution").
#   - Der Kafka-Source-Typ braucht zusätzlich das Package
#     org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 (im Argo-Workflow als
#     deps.packages gesetzt).
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
    p = argparse.ArgumentParser(description="Transform-Spec-Runner")
    p.add_argument("--spec", required=True, help="Pfad zur Pipeline-Spec-YAML")
    p.add_argument("--catalog", required=True, help="Iceberg-Catalog-Name (Spark-seitig, z.B. 'lake')")
    p.add_argument("--dry-run", action="store_true", help="Kein Write; stattdessen explain + Count")
    return p.parse_args()


def get_session(name: str) -> SparkSession:
    # Catalog-Config (spark.sql.catalog.<lake>.*) wird via spark-submit --conf
    # gesetzt — siehe argo/transform-workflow.yaml. Analog zu maintenance.py.
    return (
        SparkSession.builder.appName(f"transform-{name}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


# -----------------------------------------------------------------------------
# Spec laden + hart validieren (Fail-Fast statt Test-Suite)
# -----------------------------------------------------------------------------
def load_spec(path: str) -> dict:
    with open(path) as fh:
        spec = yaml.safe_load(fh)
    if not isinstance(spec, dict):
        raise ValueError(f"Spec {path} ist kein YAML-Mapping")

    for field in ("name", "source", "sink"):
        if field not in spec:
            raise ValueError(f"Spec: Pflichtfeld '{field}' fehlt")

    src_type = spec["source"].get("type")
    if src_type not in SOURCES:
        raise ValueError(f"Spec: unbekannter source.type '{src_type}', erlaubt: {sorted(SOURCES)}")

    sink_type = spec["sink"].get("type")
    if sink_type not in SINKS:
        raise ValueError(f"Spec: unbekannter sink.type '{sink_type}', erlaubt: {sorted(SINKS)}")

    sink_mode = spec["sink"].get("mode", "append")
    if sink_mode not in ("append", "overwrite_partitions", "merge"):
        raise ValueError(f"Spec: unbekannter sink.mode '{sink_mode}'")
    if sink_mode == "merge" and not spec["sink"].get("merge_keys"):
        raise ValueError("Spec: sink.mode=merge benötigt 'merge_keys'")

    steps = spec.get("steps") or []
    if not isinstance(steps, list):
        raise ValueError("Spec: 'steps' muss eine Liste sein")
    for i, step in enumerate(steps):
        st = step.get("type")
        if st not in STEPS:
            raise ValueError(f"Spec: Step #{i} unbekannter type '{st}', erlaubt: {sorted(STEPS)}")
        if st == "aggregate":
            for a in step.get("aggregations", []):
                if a.get("func") not in AGG_FUNCS:
                    raise ValueError(
                        f"Spec: Step #{i} unbekannte Agg-Funktion '{a.get('func')}', "
                        f"erlaubt: {sorted(AGG_FUNCS)}")

    schema = spec.get("schema")
    if schema is not None:
        if schema.get("mode") not in ("validate", "enforce"):
            raise ValueError("Spec: schema.mode muss 'validate' oder 'enforce' sein")
        if not schema.get("columns"):
            raise ValueError("Spec: schema.columns fehlt oder ist leer")
    if src_type == "kafka" and not schema:
        raise ValueError("Spec: kafka-Source benötigt einen 'schema'-Block (value-Deserialisierung)")

    return spec


# -----------------------------------------------------------------------------
# Schema-Hilfen
# -----------------------------------------------------------------------------
# Spark-DDL-Typen sind die Referenz; ein paar gängige Aliase werden normalisiert.
_TYPE_ALIASES = {
    "long": "bigint", "int": "integer", "short": "smallint",
    "byte": "tinyint", "dec": "decimal", "numeric": "decimal", "real": "float",
}


def _norm_type(t: str) -> str:
    t = str(t).lower().replace(" ", "")
    return _TYPE_ALIASES.get(t, t)


def build_ddl(columns: list[dict]) -> str:
    """Baut einen Spark-DDL-Schema-String aus schema.columns (ohne NOT NULL —
    Nullability ist lese-seitig nicht zuverlässig erzwingbar)."""
    return ", ".join(f"`{c['name']}` {c['type']}" for c in columns)


def apply_schema(df: DataFrame, schema: dict) -> DataFrame:
    """mode=validate: prüft Spalten/Typen, raised bei Abweichung.
    mode=enforce: select + cast auf das Soll-Schema."""
    mode = schema["mode"]
    columns = schema["columns"]
    actual = {f.name: f for f in df.schema.fields}

    if mode == "validate":
        for c in columns:
            name = c["name"]
            if name not in actual:
                raise ValueError(f"Schema-Validierung: Spalte '{name}' fehlt in der Source")
            want = _norm_type(c["type"])
            got = _norm_type(actual[name].dataType.simpleString())
            if want != got:
                raise ValueError(
                    f"Schema-Validierung: Spalte '{name}' hat Typ '{got}', erwartet '{want}'")
            if c.get("nullable") is False and actual[name].nullable:
                log.warning("Schema-Validierung: Spalte '%s' ist nullable, erwartet NOT NULL", name)
        if schema.get("on_extra_columns", "ignore") == "error":
            extra = sorted(set(actual) - {c["name"] for c in columns})
            if extra:
                raise ValueError(f"Schema-Validierung: unerwartete Spalten {extra}")
        return df

    # mode == "enforce"
    on_missing = schema.get("on_missing_columns", "error")
    select_cols = []
    for c in columns:
        name = c["name"]
        if name not in actual:
            if on_missing == "error":
                raise ValueError(f"Schema-Enforcement: Spalte '{name}' fehlt in der Source")
            select_cols.append(F.lit(None).cast(c["type"]).alias(name))
            continue
        select_cols.append(F.col(name).cast(c["type"]).alias(name))
    return df.select(*select_cols)


# -----------------------------------------------------------------------------
# Source-Handler — Registry SOURCES
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
    # Read-Schema nur für textbasierte Formate; parquet/orc tragen es selbst.
    if schema and fmt in ("csv", "json"):
        reader = reader.schema(build_ddl(schema["columns"]))
    # Kompression (.gz/.bz2/.zstd/...) erkennt Spark automatisch an der Endung.
    return reader.load(path)


def read_kafka(spark: SparkSession, source: dict, schema: dict | None) -> DataFrame:
    # Bewusst ein BOUNDED Batch-Read über einen festen Offset-Bereich — kein
    # kontinuierlicher Consumer. Streaming Kafka -> Iceberg bleibt Flink-Rolle
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
        # from_avro erwartet ein Avro-JSON-Schema (nicht Spark-DDL) und das
        # spark-avro-Package — in v1 noch nicht abgedeckt, siehe transform-spec.md.
        raise NotImplementedError(
            "value_format=avro ist in v1 nicht implementiert — value_format=json nutzen")
    raise ValueError(f"Unbekanntes value_format: {vfmt}")


# -----------------------------------------------------------------------------
# Step-Handler — Registry STEPS. Einheitliche Signatur (spark, df, step).
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
        # Ohne order_by: beliebige Zeile je Key — billiger, keine Window-Sort.
        return df.dropDuplicates(keys)
    # Mit order_by: deterministisch die "erste" Zeile je Key behalten.
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
    # Escape-Hatch: freie Spark-SQL gegen den bisherigen Zwischenstand (_in).
    df.createOrReplaceTempView("_in")
    return spark.sql(step["query"])


# -----------------------------------------------------------------------------
# Sink-Handler — Registry SINKS
# -----------------------------------------------------------------------------
def write_iceberg(spark: SparkSession, df: DataFrame, sink: dict, dry: bool) -> None:
    table = sink["table"]
    mode = sink.get("mode", "append")

    if dry:
        log.info("[sink %s] DRY-RUN mode=%s — kein Write, kein Snapshot", table, mode)
        df.explain(mode="formatted")
        log.info("[sink %s] DRY-RUN Ergebnis-Schema:", table)
        df.printSchema()
        # count() ist hier bewusst gewollt (Sanity-Check) — im Prod-Pfad NICHT.
        log.info("[sink %s] DRY-RUN Ergebnis-Zeilen: %d", table, df.count())
        return

    if not spark.catalog.tableExists(table):
        if sink.get("create_if_not_exists", False):
            if sink.get("partitioned_by"):
                raise ValueError(
                    f"create_if_not_exists mit partitioned_by wird in v1 nicht "
                    f"unterstützt — Tabelle {table} vorab per DDL anlegen "
                    f"(siehe iceberg/table-design.md)")
            log.info("[sink %s] Tabelle existiert nicht — wird neu angelegt", table)
            df.writeTo(table).create()
            return
        raise ValueError(
            f"Sink-Tabelle {table} existiert nicht. Vorab per DDL anlegen "
            f"(siehe iceberg/table-design.md) oder sink.create_if_not_exists setzen.")

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
        raise ValueError(f"Unbekannter sink.mode: {mode}")
    log.info("[sink %s] geschrieben (mode=%s)", table, mode)


# -----------------------------------------------------------------------------
# Registries — neuer Typ = ein Handler + ein Eintrag hier.
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
# Orchestrierung
# -----------------------------------------------------------------------------
def main() -> int:
    args = parse_args()
    spec = load_spec(args.spec)   # Fail-Fast: validiert die ganze Spec vorab

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

        # 2) Schema validieren / erzwingen
        if schema:
            df = apply_schema(df, schema)
            log.info("Schema-%s angewandt (%d Spalten)", schema["mode"], len(schema["columns"]))

        # 3) Steps sequentiell
        for i, step in enumerate(spec.get("steps") or []):
            start = time.monotonic()
            df = STEPS[step["type"]](spark, df, step)
            log.info("Step #%d %s — %.1fs", i, step["type"], time.monotonic() - start)

        # 4) Sink
        sink = spec["sink"]
        SINKS[sink["type"]](spark, df, sink, args.dry_run)

        log.info("Pipeline '%s' abgeschlossen", spec["name"])
        return 0
    except Exception as e:
        log.exception("Pipeline '%s' fehlgeschlagen: %s", spec["name"], e)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
