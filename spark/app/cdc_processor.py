import os
import json
import logging
from datetime import datetime
from typing import List, Optional
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("cdc_processor")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "admin123")
BUCKET           = os.getenv("S3_BUCKET", "northwind-data-lake")
DATABASE         = "northwind"
TABLE_CONFIG = {
    "orders":        {"pk": ["order_id"],              "ts_column": None},
    "order_details": {"pk": ["order_id", "product_id"],"ts_column": None},
    "products":      {"pk": ["product_id"],             "ts_column": None},
    "customers":     {"pk": ["customer_id"],            "ts_column": None},
}

TOPIC_PREFIX        = "northwind.public"
CHECKPOINT_PREFIX   = f"s3a://{BUCKET}/{DATABASE}/_checkpoints"  # stores last-processed offset per table


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------
def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("cdc-pipeline")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------
def get_source_path(table_name: str) -> str:
    topic = f"{TOPIC_PREFIX}.{table_name}"
    return f"s3a://{BUCKET}/topics/{topic}/"

def get_sink_path(table_name: str) -> str:
    return f"s3a://{BUCKET}/{DATABASE}/{table_name}/"

def get_checkpoint_path(table_name: str) -> str:
    return f"{CHECKPOINT_PREFIX}/{table_name}/checkpoint.json"


# ---------------------------------------------------------------------------
# Checkpoint helpers
# Checkpoints track the maximum `cdc_ts_ms` successfully written per table.
# On the next run we only read Avro records with ts_ms GREATER than this value.
# ---------------------------------------------------------------------------
def load_checkpoint(spark: SparkSession, table_name: str) -> Optional[int]:
    """Return the last max cdc_ts_ms written, or None if this is the first run."""
    checkpoint_path = get_checkpoint_path(table_name)
    try:
        df = spark.read.text(checkpoint_path)
        raw = df.collect()[0][0]
        data = json.loads(raw)
        ts = data.get("last_ts_ms")
        logger.info("Checkpoint for '%s': last_ts_ms=%s", table_name, ts)
        return ts
    except Exception:
        logger.info("No checkpoint found for '%s', will do full initial load.", table_name)
        return None

def save_checkpoint(spark: SparkSession, table_name: str, max_ts_ms: int) -> None:
    """Persist the max cdc_ts_ms so the next run knows where to start."""
    checkpoint_path = get_checkpoint_path(table_name)
    payload = json.dumps({"last_ts_ms": max_ts_ms, "updated_at": datetime.utcnow().isoformat()})
    # Write as a single-row text file; coalesce(1) ensures exactly one file.
    spark.createDataFrame([(payload,)], ["value"]) \
         .coalesce(1) \
         .write.mode("overwrite").text(checkpoint_path)
    logger.info("Checkpoint saved for '%s': last_ts_ms=%d", table_name, max_ts_ms)


# ---------------------------------------------------------------------------
# Partition column helpers (unchanged from original)
# ---------------------------------------------------------------------------
def add_partition_columns(df: DataFrame, table_name: str) -> DataFrame:
    ts_column = TABLE_CONFIG[table_name].get("ts_column")
    if ts_column and ts_column in df.columns:
        col_type = df.schema[ts_column].dataType.typeName()
        logger.info("Using business timestamp '%s' (type: %s) for partitioning", ts_column, col_type)
        sample = df.select(ts_column).limit(3).collect()
        sample_val = sample[0][ts_column] if sample else 0
        if sample_val and sample_val > 1_000_000_000:
            date_col = F.to_date(F.from_unixtime(F.col(ts_column)))
        elif sample_val and 10_000 < sample_val < 100_000:
            date_col = F.to_date(F.lit("1970-01-01").cast("date") + F.col(ts_column).cast("int"))
        else:
            date_col = F.to_date(F.col(ts_column).cast("string"))
    else:
        logger.info("Using CDC timestamp 'cdc_ts_ms' for partitioning")
        date_col = F.to_date(F.from_unixtime(F.col("cdc_ts_ms") / 1000))

    return df.withColumns({
        "year":  F.year(date_col),
        "month": F.month(date_col),
        "day":   F.dayofmonth(date_col),
    })


# ---------------------------------------------------------------------------
# Core processing logic
# ---------------------------------------------------------------------------
def wait_for_source_path(spark: SparkSession, path: str,
                          max_retries: int = 5, delay: int = 10) -> bool:
    for i in range(max_retries):
        try:
            spark.read.format("avro").option("recursiveFileLookup", "true") \
                 .load(path).limit(1).count()
            logger.info("Source path is ready: %s", path)
            return True
        except Exception as e:
            logger.warning("Source not ready (attempt %d/%d): %s", i + 1, max_retries, e)
            if i < max_retries - 1:
                time.sleep(delay)
    return False


def read_new_avro_records(spark: SparkSession, source_path: str,
                           last_ts_ms: Optional[int]) -> Optional[DataFrame]:
    """
    INCREMENTAL READ
    ----------------
    Read all Avro files under source_path, then immediately filter to only
    rows with ts_ms > last_ts_ms (i.e. rows we haven't processed yet).

    Why filter after reading rather than selecting specific files?
    The S3 Sink Connector organises files by Kafka offset/time, not by ts_ms.
    Filtering by column value after loading is simpler and still correct because
    Spark only materialises matching partitions when it scans the data.
    """
    try:
        raw_df = (
            spark.read
            .format("avro")
            .option("recursiveFileLookup", "true")
            .load(source_path)
        )
    except Exception as e:
        logger.warning("Cannot read source path %s: %s", source_path, e)
        return None

    if raw_df.isEmpty():
        logger.info("No Avro data found at %s", source_path)
        return None

    # Apply incremental filter when a checkpoint exists
    if last_ts_ms is not None:
        raw_df = raw_df.filter(F.col("ts_ms") > last_ts_ms)
        logger.info("Incremental filter: ts_ms > %d", last_ts_ms)
    else:
        logger.info("No checkpoint — performing full initial load")

    return raw_df


def extract_cdc_events(raw_df: DataFrame, pk_cols: List[str]) -> Optional[DataFrame]:
    """
    Parse Debezium envelope and deduplicate — latest record wins per PK.
    Handles:
      c  = create (INSERT)
      u  = update (UPDATE)
      r  = read   (snapshot)
      d  = delete → dropped (handled by merge step below)
    """
    # Keep only live rows; ignore deletes at this stage
    df_filtered = raw_df.filter(
        F.col("op").isin("c", "u", "r") & F.col("after").isNotNull()
    )
    if df_filtered.isEmpty():
        return None

    after_fields = df_filtered.schema["after"].dataType.fieldNames()
    df_flat = df_filtered.select(
        *[F.col(f"after.{f}").alias(f) for f in after_fields],
        F.col("ts_ms").alias("cdc_ts_ms"),
        F.col("op").alias("cdc_op"),
    )

    # Deduplicate: keep the latest event per primary key within the new batch
    window_spec = Window.partitionBy(*pk_cols).orderBy(F.col("cdc_ts_ms").desc())
    return (
        df_flat
        .withColumn("_rank", F.row_number().over(window_spec))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def merge_with_existing(spark: SparkSession,
                         new_df: DataFrame,
                         sink_path: str,
                         pk_cols: List[str]) -> DataFrame:
    """
    MERGE (latest-wins join)
    ------------------------
    For every partition touched by new_df, combine existing Parquet rows with
    the incoming CDC rows and keep only the most-recent record per PK.

    This is a "poor man's MERGE" that avoids Delta Lake / Iceberg dependencies.
    It works because:
      1. We only read the partitions that new_df will overwrite (partition pruning).
      2. We union old + new, then deduplicate by PK keeping max(cdc_ts_ms).
      3. We write back ONLY those partitions (dynamic overwrite).

    For tables with delete events you would union in a deleted-PKs filter here
    and remove matching rows before writing.
    """
    try:
        existing_df = (
            spark.read
            .format("parquet")
            .load(sink_path)
        )
    except Exception:
        # Sink does not exist yet — first run, no merge needed
        logger.info("Sink path not found, skipping merge (first write).")
        return new_df

    # Find which year/month/day partitions are in the new batch
    touched_partitions = (
        new_df.select("year", "month", "day")
              .distinct()
              .collect()
    )
    if not touched_partitions:
        return new_df

    # Build a filter expression to select only the affected partitions from existing data
    partition_filter = F.lit(False)
    for row in touched_partitions:
        partition_filter = partition_filter | (
            (F.col("year")  == row["year"]) &
            (F.col("month") == row["month"]) &
            (F.col("day")   == row["day"])
        )

    existing_touched = existing_df.filter(partition_filter)

    logger.info("Merging %d new records with existing data in %d partition(s)",
                new_df.count(), len(touched_partitions))

    # Union and deduplicate: latest cdc_ts_ms wins per PK
    window_spec = Window.partitionBy(*pk_cols).orderBy(F.col("cdc_ts_ms").desc())
    merged = (
        existing_touched.union(new_df)
        .withColumn("_rank", F.row_number().over(window_spec))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    return merged


def register_partitions(spark, df, table_name):
    """
    Register partitions in Hive Metastore based on DataFrame content
    """
    partitions = (
        df.select("year", "month", "day")
          .distinct()
          .collect()
    )

    if not partitions:
        return
    partition_sql = ",\n".join([
        f"PARTITION (year={p['year']}, month={p['month']}, day={p['day']})"
        for p in partitions
    ])

    spark.sql(f"""
        ALTER TABLE {DATABASE}.{table_name}
        ADD IF NOT EXISTS
        {partition_sql}
    """)


def process_table(spark: SparkSession, table_name: str, pk_cols: List[str]) -> None:
    """
    Full incremental pipeline for a single table:
      1. Load checkpoint (last processed ts_ms)
      2. Read only new Avro records since that checkpoint
      3. Parse + deduplicate CDC events
      4. Add partition columns
      5. Merge new records with existing Parquet (per affected partition)
      6. Write back only the changed partitions (partial overwrite)
      7. Save updated checkpoint
    """
    source_path = get_source_path(table_name)
    sink_path   = get_sink_path(table_name)

    logger.info("=" * 50)
    logger.info("Processing table: %s", table_name)

    if not wait_for_source_path(spark, source_path):
        logger.warning("Skipping '%s' — source path unavailable", table_name)
        return

    # --- Step 1: Checkpoint ---
    last_ts_ms = load_checkpoint(spark, table_name)

    # --- Step 2: Incremental read ---
    raw_df = read_new_avro_records(spark, source_path, last_ts_ms)
    if raw_df is None or raw_df.isEmpty():
        logger.info("No new records for '%s', nothing to do.", table_name)
        return

    new_record_count = raw_df.count()
    logger.info("New Avro records for '%s': %d", table_name, new_record_count)

    # --- Step 3: Parse & deduplicate ---
    new_df = extract_cdc_events(raw_df, pk_cols)
    if new_df is None or new_df.isEmpty():
        logger.info("No insertable rows after CDC filter for '%s'.", table_name)
        return

    logger.info("Deduplicated new records: %d", new_df.count())

    # --- Step 4: Add partition columns ---
    new_df = add_partition_columns(new_df, table_name)
    # --- Extract partitions
    partitions = (
        new_df.select("year", "month", "day")
              .distinct()
              .collect()
    )

    # --- Step 5: Merge with existing Parquet (partial partitions only) ---
    merged_df = merge_with_existing(spark, new_df, sink_path, pk_cols)

    # --- Step 6: Partial overwrite ---
    # dynamic partitionOverwriteMode (set on SparkSession) ensures only the
    # partitions present in merged_df are overwritten; others are untouched.
    (
        merged_df
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("year", "month", "day")
        .parquet(sink_path)
    )
    logger.info("Wrote Parquet for '%s' → %s", table_name, sink_path)

    # --- Step 7: Save checkpoint ---
    max_ts_ms = raw_df.agg(F.max("ts_ms")).collect()[0][0]
    if max_ts_ms:
        save_checkpoint(spark, table_name, int(max_ts_ms))

    # --- Step 8: Sync with Hive Metastore
    register_partitions(spark, partitions, table_name)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info("=" * 60)
    logger.info("CDC Processor starting  (incremental Avro → Parquet)")
    logger.info("MinIO endpoint : %s", MINIO_ENDPOINT)
    logger.info("Bucket         : %s", BUCKET)
    logger.info("=" * 60)

    spark = build_spark_session()

    for table_name, cfg in TABLE_CONFIG.items():
        try:
            process_table(spark, table_name, cfg["pk"])
        except Exception as e:
            logger.error("Unhandled error for '%s': %s", table_name, e, exc_info=True)

    spark.stop()
    logger.info("CDC Processor run complete.")


if __name__ == "__main__":
    main()