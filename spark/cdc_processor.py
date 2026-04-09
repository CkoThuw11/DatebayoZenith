"""
CDC Processor: Avro → Parquet
-----------------------------
Reads Debezium CDC Avro files from MinIO (S3) written by the Kafka S3 Sink connector,
deduplicates records using last-write-wins per primary key, and writes clean Parquet
files back to MinIO under the `processed/` prefix.

Runs as a batch on a schedule (called from entrypoint.sh every N minutes).
Uses checkpointing to track already-ingested files (incremental, not full re-scan).
"""

import os
import logging
from typing import List

from pyspark.sql import SparkSession
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
MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY  = os.getenv("MINIO_ROOT_PASSWORD", "admin123")
BUCKET            = os.getenv("S3_BUCKET", "northwind-data-lake")
CHECKPOINT_BASE   = os.getenv("CHECKPOINT_BASE", "/tmp/spark-checkpoints")

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
        # S3A connector settings
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Avro support
        .config("spark.sql.extensions", "org.apache.spark.sql.avro.AvroExtensions")
        # Shuffle tuning for small-scale local workloads
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_source_path(table_name: str) -> str:
    topic = f"{TOPIC_PREFIX}.{table_name}"
    return f"s3a://{BUCKET}/topics/{topic}/"


def get_sink_path(table_name: str) -> str:
    return f"s3a://{BUCKET}/processed/{table_name}/"


def get_checkpoint_path(table_name: str) -> str:
    return f"{CHECKPOINT_BASE}/{table_name}/"


def process_table(spark: SparkSession, table_name: str, pk_cols: List[str]) -> None:
    """
    Read new Avro files for a single table, extract the latest row per PK,
    and upsert (overwrite partition) into the Parquet sink.
    """
    source_path     = get_source_path(table_name)
    sink_path       = get_sink_path(table_name)
    checkpoint_path = get_checkpoint_path(table_name)

    logger.info("Processing table '%s' from %s", table_name, source_path)

    # ------------------------------------------------------------------
    # 1. Read ALL Avro files (Spark tracks new files via checkpoint dir)
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
    # 2. Explode the Debezium envelope
    #    The S3 Sink with AvroFormat writes the full Debezium message as the
    #    value, which includes: before, after, source, op, ts_ms
    # ------------------------------------------------------------------
    # op: c=create, r=read(snapshot), u=update, d=delete
    # We keep 'after' for c/u/r and drop deletes (op='d')
    df_filtered = raw_df.filter(
        (F.col("op").isin("c", "u", "r")) & F.col("after").isNotNull()
    )

    if df_filtered.isEmpty():
        logger.info("No insertable rows for table '%s' after op filter.", table_name)
        return

    # Flatten the 'after' struct into top-level columns
    after_fields = df_filtered.schema["after"].dataType.fieldNames()
    df_flat = df_filtered.select(
        *[F.col(f"after.{field}").alias(field) for field in after_fields],
        F.col("ts_ms"),
        F.col("op"),
    )

    # ------------------------------------------------------------------
    # 3. Deduplicate — last write wins per primary key
    # ------------------------------------------------------------------
    window_spec = Window.partitionBy(*pk_cols).orderBy(F.col("ts_ms").desc())
    df_deduped = (
        df_flat
        .withColumn("_rank", F.row_number().over(window_spec))
        .filter(F.col("_rank") == 1)
        .drop("_rank", "ts_ms", "op")   # clean up internal metadata cols
    )

    logger.info(
        "Deduplicated record count for '%s': %d",
        table_name,
        df_deduped.count(),
    )

    # ------------------------------------------------------------------
    # 4. Write to Parquet (overwrite — full snapshot of current state)
    #    Trino can query this path directly with a connector.
    # ------------------------------------------------------------------
    (
        df_deduped
        .coalesce(1)          # small data: one file per table for simplicity
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(sink_path)
    )

    logger.info("Successfully wrote Parquet for table '%s' → %s", table_name, sink_path)


def main() -> None:
    logger.info("=" * 60)
    logger.info("CDC Processor starting (Avro → Parquet)")
    logger.info("Source bucket : %s", BUCKET)
    logger.info("MinIO endpoint: %s", MINIO_ENDPOINT)
    logger.info("=" * 60)

    spark = build_spark_session()

    for table_name, cfg in TABLE_CONFIG.items():
        try:
            process_table(spark, table_name, cfg["pk"])
        except Exception as e:
            logger.error("Unhandled error processing table '%s': %s", table_name, e, exc_info=True)

    spark.stop()
    logger.info("CDC Processor run complete.")


if __name__ == "__main__":
    main()
