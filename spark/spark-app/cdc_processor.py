import json
import os
import time
import logging
from datetime import datetime, timezone
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

import alert
from deequ_checks import run_dq_checks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("cdc_processor")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
GRAFANA_BASE_URL = os.getenv("GRAFANA_BASE_URL", "http://grafana:3000")
RAW_BUCKET    = os.getenv("RAW_BUCKET",    "raw")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
FRESHNESS_THRESHOLD_SECONDS = int(os.getenv("FRESHNESS_THRESHOLD_SECONDS", "900"))
PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "pushgateway:9091")
DATABASE = "northwind"
CHECKPOINT_PREFIX = f"northwind/_checkpoints"
TABLE_CONFIG = {
    "orders":        {"pk": ["order_id"]},
    "order_details": {"pk": ["order_id", "product_id"]},
    "products":      {"pk": ["product_id"]},
}


registry = CollectorRegistry()
g_rows      = Gauge("cdc_rows_written_total",          "New deduplicated rows written this run", ["table"],          registry=registry)
g_duration  = Gauge("cdc_processing_duration_seconds", "Processing duration per table",          ["table"],          registry=registry)
g_timestamp = Gauge("cdc_last_run_timestamp_seconds",  "Unix timestamp of last successful run",  ["table"],          registry=registry)
g_dq_status = Gauge("cdc_dq_check_status",             "DQ check status (1=pass, 0=fail)",       ["table", "check"], registry=registry)
g_rows_read = Gauge("cdc_rows_read_total", "Rows read from raw before dedupe", ["table"], registry=registry)
g_dq_last_run = Gauge("cdc_dq_last_run_timestamp_seconds", "Unix timestamp of last DQ run", ["table"], registry=registry)

g_freshness_gap = Gauge(
    "cdc_freshness_gap_seconds",
    "Seconds since the most recent CDC event was written to bronze (event time lag)",
    ["table"],
    registry=registry,
)

g_partition_registered = Gauge(
    "cdc_partition_registered",
    "Hive partition registration status (1=success, 0=failed)",
    ["table"],
    registry=registry,
)

g_consecutive_failures = Gauge(
    "cdc_consecutive_failures_total",
    "Consecutive Spark run failures for this table without a successful write",
    ["table"],
    registry=registry,
)

_consecutive_failures: dict = {table: 0 for table in TABLE_CONFIG}


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("cdc-avro-to-parquet")
        .config("spark.hadoop.fs.s3a.endpoint",                    MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",                  MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",                  MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",           "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled",      "false")
        .config("spark.hadoop.fs.s3a.endpoint.region",             "us-east-1")
        .config("spark.hadoop.fs.s3a.impl",                        "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.fast.upload",                 "true")
        .config("spark.sql.shuffle.partitions",                    "4")
        .config("spark.hadoop.hive.metastore.uris",                "thrift://hive-metastore:9083")
        .config("spark.sql.catalogImplementation",                 "hive")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_source_path(table_name: str) -> str:
    return f"s3a://{RAW_BUCKET}/northwind/{table_name}/"

def get_sink_path(table_name: str) -> str:
    return f"s3a://{BRONZE_BUCKET}/northwind/{table_name}/"

def get_checkpoint_path(table_name: str) -> str:
    return f"s3a://{BRONZE_BUCKET}/{CHECKPOINT_PREFIX}/{table_name}.json"



def _get_hadoop_fs(spark: SparkSession, path: str):
    """Return Hadoop FileSystem handle for the given S3A path."""
    jvm  = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    uri  = jvm.java.net.URI(path)
    return jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)

def load_checkpoint(spark: SparkSession, table_name: str) -> Optional[int]:
    """
    Read last_processed_ts_ms from checkpoint file.
    Returns None on first run (no checkpoint file exists yet).
    """
    path = get_checkpoint_path(table_name)
    try:
        fs       = _get_hadoop_fs(spark, path)
        jpath    = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
        if not fs.exists(jpath):
            logger.info("No checkpoint found for '%s' — full backfill will run.", table_name)
            return None
        stream   = fs.open(jpath)
        reader   = spark.sparkContext._jvm.java.io.BufferedReader(
                       spark.sparkContext._jvm.java.io.InputStreamReader(stream))
        content  = ""
        line = reader.readLine()
        while line is not None:
            content += line
            line = reader.readLine()
        reader.close()
        data = json.loads(content)
        ts   = int(data["last_processed_ts_ms"])
        logger.info(
            "Checkpoint loaded for '%s': last_processed_ts_ms=%d (%s)",
            table_name, ts,
            datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat(),
        )
        return ts
    except Exception as e:
        logger.warning("Failed to load checkpoint for '%s': %s — falling back to full backfill.", table_name, e)
        return None

def save_checkpoint(spark: SparkSession, table_name: str, last_ts_ms: int) -> None:
    """
    Write last_processed_ts_ms to checkpoint file.
    Called only after a successful Parquet write — never on failure.
    """
    path = get_checkpoint_path(table_name)
    try:
        fs      = _get_hadoop_fs(spark, path)
        jpath   = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
        stream  = fs.create(jpath, True)  # overwrite=True
        writer  = spark.sparkContext._jvm.java.io.PrintWriter(
                      spark.sparkContext._jvm.java.io.OutputStreamWriter(stream, "UTF-8"))
        payload = json.dumps({"table": table_name, "last_processed_ts_ms": last_ts_ms})
        writer.print(payload)
        writer.close()
        logger.info(
            "Checkpoint saved for '%s': last_processed_ts_ms=%d (%s)",
            table_name, last_ts_ms,
            datetime.fromtimestamp(last_ts_ms / 1000, tz=timezone.utc).isoformat(),
        )
    except Exception as e:

        logger.error("Failed to save checkpoint for '%s': %s", table_name, e)



def register_hive_partition(
    spark: SparkSession,
    table_name: str,
    year: int,
    month: int,
    day: int,
) -> bool:
    """
    Register year/month/day partition in Hive Metastore after Parquet write.
    Returns True on success (including already-exists), False on failure.
    Idempotent — checks SHOW PARTITIONS before issuing ALTER TABLE.
    """
    full_table     = f"{DATABASE}.{table_name}"
    partition_path = f"{get_sink_path(table_name)}year={year}/month={month}/day={day}"

    try:
        existing      = spark.sql(f"SHOW PARTITIONS {full_table}").collect()
        existing_keys = {row[0] for row in existing}
        canonical     = f"year={year}/month={month}/day={day}"

        if canonical in existing_keys:
            logger.info("Partition already registered, skipping: %s [%s]", full_table, canonical)
            return True

        spark.sql(f"""
            ALTER TABLE {full_table}
            ADD PARTITION (year={year}, month={month}, day={day})
            LOCATION '{partition_path}'
        """)
        logger.info("Partition registered: %s [%s] → %s", full_table, canonical, partition_path)
        return True

    except Exception as e:
        logger.error("Failed to register partition for %s [year=%d/month=%d/day=%d]: %s", full_table, year, month, day, e)
        return False


def process_table(spark: SparkSession, table_name: str, pk_cols: List[str]) -> None:
    """
    1.  Load checkpoint → get last_processed_ts_ms (None = first run)
    2.  Read Avro from raw/northwind/{table}/
    3.  Filter to new events only: cdc_ts_ms > last_processed_ts_ms
    4.  Filter + flatten Debezium envelope (op in c/u/r, after not null)
    5.  Deduplicate (last-write-wins per PK within this batch)
    6.  Run DQ checks — abort write on failure
    7.  Add partition columns (year/month/day as INT)
    8.  Write Parquet to bronze/northwind/{table}/
    9.  Register partition in Hive Metastore
    10. Save checkpoint with MAX(cdc_ts_ms) from this batch
    11. Push metrics: freshness_gap, partition_registered, consecutive_failures
    """
    source_path = get_source_path(table_name)
    sink_path   = get_sink_path(table_name)

    logger.info("Processing table '%s' | source: %s", table_name, source_path)

    start_time   = time.time()
    rows_written = 0
    rows_read    = 0

    try:
        # ── 1. Load checkpoint ────────────────────────────────────────────
        last_processed_ts_ms = load_checkpoint(spark, table_name)

        # ── 2. Read all Avro files ────────────────────────────────────────
        try:
            raw_df = (
                spark.read
                .format("avro")
                .option("recursiveFileLookup", "true")
                .load(source_path)
            )
        except Exception as e:
            logger.warning("Cannot read source path %s — skipping. Error: %s", source_path, e)
            return

        if raw_df.isEmpty():
            logger.info("No data for table '%s', skipping.", table_name)
            return

        # ── 3. Incremental filter — only new events since last checkpoint ─
        if last_processed_ts_ms is not None:
            raw_df = raw_df.filter(F.col("ts_ms") > last_processed_ts_ms)
            if raw_df.isEmpty():
                logger.info(
                    "No new events for '%s' since checkpoint ts=%d — skipping.",
                    table_name, last_processed_ts_ms
                )
                return
            logger.info(
                "Incremental filter applied for '%s': ts_ms > %d",
                table_name, last_processed_ts_ms
            )

        # ── 4. Filter + flatten Debezium envelope ─────────────────────────
        df_filtered = raw_df.filter(
            F.col("op").isin("c", "u", "r") & F.col("after").isNotNull()
        )
        if df_filtered.isEmpty():
            logger.info("No insertable rows for '%s' after op filter.", table_name)
            return

        after_fields = df_filtered.schema["after"].dataType.fieldNames()
        df_flat = df_filtered.select(
            *[F.col(f"after.{field}").alias(field) for field in after_fields],
            F.col("ts_ms").alias("cdc_ts_ms"),
            F.col("op").alias("cdc_op"),
        )

        # ── 5. Deduplicate — last write wins per PK within this batch ─────
        window_spec = Window.partitionBy(*pk_cols).orderBy(F.col("cdc_ts_ms").desc())
        df_deduped = (
            df_flat
            .withColumn("_rank", F.row_number().over(window_spec))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )
        rows_read = df_flat.count()
        rows_written = df_deduped.count()
        logger.info("Deduplicated record count for '%s': %d", table_name, rows_written)

        # ── 6. DQ checks BEFORE write ─────────────────────────────────────
        dq_passed = run_dq_checks(
            spark, df_deduped, table_name, pk_cols, g_dq_status,
            freshness_threshold_seconds=FRESHNESS_THRESHOLD_SECONDS,
            consecutive_failures=_consecutive_failures[table_name],
            dashboard_url=f"{GRAFANA_BASE_URL}/d/cdc-pipeline?var-table={table_name}",
        )
        if not dq_passed:
            logger.error("DQ checks failed for '%s' — aborting write.", table_name)
            _consecutive_failures[table_name] += 1
            g_consecutive_failures.labels(table=table_name).set(_consecutive_failures[table_name])
            return

        # ── 7. Add partition columns ──────────────────────────────────────
        now_utc = datetime.now(timezone.utc)
        df_partitioned = df_deduped.withColumns({
            "year":  F.lit(now_utc.year).cast("int"),
            "month": F.lit(now_utc.month).cast("int"),
            "day":   F.lit(now_utc.day).cast("int"),
        })

        # ── 8. Write Parquet ──────────────────────────────────────────────
        (
            df_partitioned.write
            .mode("append")
            .option("compression", "snappy")
            .partitionBy("year", "month", "day")
            .parquet(sink_path)
        )
        logger.info(
            "Wrote %d rows for '%s' → %s (year=%d/month=%02d/day=%02d)",
            rows_written, table_name, sink_path, now_utc.year, now_utc.month, now_utc.day,
        )

        # ── 9. Register partition in Hive Metastore ───────────────────────
        partition_ok = register_hive_partition(spark, table_name, now_utc.year, now_utc.month, now_utc.day)
        g_partition_registered.labels(table=table_name).set(1 if partition_ok else 0)

        # ── 10. Save checkpoint ───────────────────────────────────────────
        max_ts_row = df_deduped.agg(F.max("cdc_ts_ms").alias("max_ts")).collect()[0]
        if max_ts_row["max_ts"] is not None:
            new_checkpoint_ts = int(max_ts_row["max_ts"])
            save_checkpoint(spark, table_name, new_checkpoint_ts)
            logger.info(
                "Checkpoint advanced for '%s': %d → %d",
                table_name, last_processed_ts_ms or 0, new_checkpoint_ts
            )

        # Reset consecutive failure counter on success
        _consecutive_failures[table_name] = 0
        g_consecutive_failures.labels(table=table_name).set(0)

    finally:
        duration = time.time() - start_time
        g_rows.labels(table=table_name).set(rows_written)
        g_rows_read.labels(table=table_name).set(rows_read)
        g_duration.labels(table=table_name).set(duration)
        g_dq_last_run.labels(table=table_name).set(time.time())
        g_timestamp.labels(table=table_name).set(time.time())

        # ── Freshness gap — always computed, even on DQ failure or exception ─
        try:
            checkpoint_ts = load_checkpoint(spark, table_name)
            if checkpoint_ts is not None:
                now_epoch_ms    = datetime.now(timezone.utc).timestamp() * 1000
                freshness_gap_s = (now_epoch_ms - checkpoint_ts) / 1000.0
                g_freshness_gap.labels(table=table_name).set(freshness_gap_s)
                logger.info("Freshness gap for '%s': %.1f seconds", table_name, freshness_gap_s)

                if freshness_gap_s > FRESHNESS_THRESHOLD_SECONDS:
                    alert.send_alert(
                        summary=f"[CDC Freshness] '{table_name}' data is {freshness_gap_s / 60:.1f} min stale (threshold: {FRESHNESS_THRESHOLD_SECONDS / 60:.0f} min)",
                        severity="critical",
                        table=table_name,
                        error_type="stale_data",
                        group="data-quality",
                        component="spark/freshness_check",
                        details={
                            "consecutive_failures":  _consecutive_failures[table_name],
                            "freshness_gap_seconds": round(freshness_gap_s, 1),
                            "threshold_seconds":     FRESHNESS_THRESHOLD_SECONDS,
                            "dashboard_url":         f"{GRAFANA_BASE_URL}/d/cdc-pipeline?var-table={table_name}",
                        },
                    )
                else:
                    alert.resolve_alert(table=table_name, error_type="stale_data")
        except Exception as e:
            logger.warning("Failed to compute freshness gap for '%s': %s", table_name, e)



def main() -> None:
    logger.info("=" * 60)
    logger.info("CDC Processor starting  (Avro → Parquet, incremental)")
    logger.info("Source      : s3a://%s/northwind/", RAW_BUCKET)
    logger.info("Sink        : s3a://%s/northwind/", BRONZE_BUCKET)
    logger.info("Checkpoints : s3a://%s/%s/", BRONZE_BUCKET, CHECKPOINT_PREFIX)
    logger.info("MinIO       : %s", MINIO_ENDPOINT)
    logger.info("PagerDuty   : %s", "configured" if alert.PAGERDUTY_ROUTING_KEY else "NOT SET")
    logger.info("=" * 60)

    spark = build_spark_session()

    for table_name, cfg in TABLE_CONFIG.items():
        try:
            process_table(spark, table_name, cfg["pk"])
        except Exception as e:
            logger.error("Unhandled error for table '%s': %s", table_name, e, exc_info=True)
            _consecutive_failures[table_name] += 1
            g_consecutive_failures.labels(table=table_name).set(_consecutive_failures[table_name])
            failed_step = getattr(e, "_cdc_step", "unknown")
            alert.send_alert(
                summary=f"[CDC Pipeline] '{table_name}' crashed at step {failed_step}: {type(e).__name__}",
                severity="critical",
                table=table_name,
                error_type="unhandled_exception",
                group="infrastructure",
                component="spark",
                details={
                    "consecutive_failures": _consecutive_failures[table_name],
                    "failed_step":          failed_step,
                    "exception":            str(e),
                    "dashboard_url":        f"{GRAFANA_BASE_URL}/d/cdc-pipeline?var-table={table_name}",
                },
            )

    spark.stop()

    try:
        push_to_gateway(PUSHGATEWAY_URL, job="cdc_processor", registry=registry)
        logger.info("Metrics pushed to Pushgateway: %s", PUSHGATEWAY_URL)
    except Exception as e:
        logger.error("Failed to push metrics to Pushgateway: %s", e)

    logger.info("CDC Processor run complete.")
    os._exit(0)


if __name__ == "__main__":
    main()