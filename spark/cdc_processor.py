"""
CDC Processor: Avro → Parquet
-----------------------------
Reads Debezium CDC Avro files from MinIO (S3) written by the Kafka S3 Sink connector,
deduplicates records using last-write-wins per primary key, and writes clean Parquet
files back to MinIO under the `parquet/` prefix — partitioned by year/month/day.

Runs as a batch on a schedule (called from entrypoint.sh every N minutes).
"""

import os
import logging
from datetime import datetime, timezone
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("cdc_processor")

# ---------------------------------------------------------------------------
# Configuration (injected via environment variables from docker-compose)
# ---------------------------------------------------------------------------
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
BUCKET           = os.getenv("S3_BUCKET", "northwind-data-lake")

# Tables to process and their primary keys
TABLE_CONFIG = {
    "orders":        {"pk": ["order_id"]},
    "order_details": {"pk": ["order_id", "product_id"]},
    "products":      {"pk": ["product_id"]},
    "customers":     {"pk": ["customer_id"]},
}

TOPIC_PREFIX = "northwind.public"


def build_spark_session() -> SparkSession:
    """Build and configure a SparkSession with S3A / MinIO support."""
    spark = (
        SparkSession.builder.appName("cdc-avro-to-parquet")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.extensions", "org.apache.spark.sql.avro.AvroExtensions")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_source_path(table_name: str) -> str:
    """Avro files written by S3 Sink Connector."""
    topic = f"{TOPIC_PREFIX}.{table_name}"
    return f"s3a://{BUCKET}/topics/{topic}/"


def get_sink_path(table_name: str) -> str:
    # FIX C1: Changed 'processed/' → 'parquet/' to match Trino external_location
    return f"s3a://{BUCKET}/parquet/{table_name}/"


def process_table(spark: SparkSession, table_name: str, pk_cols: List[str]) -> None:
    """
    Read Avro files for a single table, extract latest row per PK,
    add partition columns, and write partitioned Parquet for Trino to query.
    """
    source_path = get_source_path(table_name)
    sink_path   = get_sink_path(table_name)

    logger.info("Processing table '%s' from %s", table_name, source_path)

    # ------------------------------------------------------------------
    # 1. Read Avro files (S3 Sink AvroFormat — full Debezium envelope)
    # ------------------------------------------------------------------
    try:
        raw_df = (
            spark.read
            .format("avro")
            .option("recursiveFileLookup", "true")
            .load(source_path)
        )
    except Exception as e:
        logger.warning("Could not read source path %s – skipping. Error: %s", source_path, e)
        return

    if raw_df.isEmpty():
        logger.info("No data found for table '%s', skipping.", table_name)
        return

    logger.info("Raw record count for '%s': %d", table_name, raw_df.count())

    # ------------------------------------------------------------------
    # 2. Explode Debezium envelope — keep c/u/r, drop deletes (op='d')
    # ------------------------------------------------------------------
    df_filtered = raw_df.filter(
        (F.col("op").isin("c", "u", "r")) & F.col("after").isNotNull()
    )

    if df_filtered.isEmpty():
        logger.info("No insertable rows for '%s' after op filter.", table_name)
        return

    # Flatten 'after' struct into top-level columns
    after_fields = df_filtered.schema["after"].dataType.fieldNames()
    df_flat = df_filtered.select(
        *[F.col(f"after.{field}").alias(field) for field in after_fields],
        # FIX C3: Rename ts_ms → cdc_ts_ms, op → cdc_op to match Trino DDL
        F.col("ts_ms").alias("cdc_ts_ms"),
        F.col("op").alias("cdc_op"),
    )

    # ------------------------------------------------------------------
    # 3. Deduplicate — last write wins per primary key (by ts_ms)
    # ------------------------------------------------------------------
    window_spec = Window.partitionBy(*pk_cols).orderBy(F.col("cdc_ts_ms").desc())
    df_deduped = (
        df_flat
        .withColumn("_rank", F.row_number().over(window_spec))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        # cdc_op and cdc_ts_ms are KEPT — Trino DDL expects them
    )

    logger.info(
        "Deduplicated record count for '%s': %d",
        table_name,
        df_deduped.count(),
    )

    # ------------------------------------------------------------------
    # 4. FIX C2: Add partition columns year / month / day
    #    Trino DDL: partitioned_by = ARRAY['year', 'month', 'day']
    # ------------------------------------------------------------------
    now_utc = datetime.now(timezone.utc)
    df_partitioned = df_deduped.withColumns({
        "year":  F.lit(str(now_utc.year)),
        "month": F.lit(str(now_utc.month).zfill(2)),
        "day":   F.lit(str(now_utc.day).zfill(2)),
    })

    # ------------------------------------------------------------------
    # 5. Write Parquet — partitioned by year/month/day, Snappy compressed
    #    Trino reads this path via hive.northwind.<table> external table
    # ------------------------------------------------------------------
    (
        df_partitioned
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("year", "month", "day")  # creates year=X/month=XX/day=XX subfolders
        .parquet(sink_path)
    )

    logger.info(
        "Wrote Parquet for '%s' → %s  (partition: year=%s/month=%s/day=%s)",
        table_name, sink_path,
        now_utc.year, str(now_utc.month).zfill(2), str(now_utc.day).zfill(2),
    )


def main() -> None:
    logger.info("=" * 60)
    logger.info("CDC Processor starting  (Avro → Parquet)")
    logger.info("Source bucket : %s", BUCKET)
    logger.info("MinIO endpoint: %s", MINIO_ENDPOINT)
    logger.info("Sink prefix   : parquet/")
    logger.info("=" * 60)

    spark = build_spark_session()

    for table_name, cfg in TABLE_CONFIG.items():
        try:
            process_table(spark, table_name, cfg["pk"])
        except Exception as e:
            logger.error(
                "Unhandled error processing table '%s': %s",
                table_name, e, exc_info=True,
            )

    spark.stop()
    logger.info("CDC Processor run complete.")


if __name__ == "__main__":
    main()