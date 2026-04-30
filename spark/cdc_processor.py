"""
CDC Processor: Avro → Parquet
-----------------------------
Reads Debezium CDC Avro files from MinIO (S3) written by the Kafka S3 Sink connector,
deduplicates records using last-write-wins per primary key, and writes clean Parquet
files back to MinIO under the `parquet/` prefix — partitioned by year/month/day.

Data Quality: After each Avro→Parquet cycle, Amazon Deequ checks are run per table:
  • Null PK check        — no primary key column may be NULL
  • Uniqueness PK check  — no duplicate primary keys
  • Valid cdc_op check   — cdc_op must be one of {c, u, r}
  • Non-empty check      — table must have at least 1 row

Alerting: On check failure or unhandled exception, alert.py fires a PagerDuty incident.
          Graceful degradation when PAGERDUTY_ROUTING_KEY is not set.

Runs as a batch on a schedule (called from entrypoint.sh every N minutes).
"""

import os
import time
import logging
from datetime import datetime, timezone
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

# Tích hợp Prometheus và Kafka Client cho Monitoring
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from kafka import KafkaConsumer, TopicPartition

# Data Quality
from dataclasses import dataclass

# PagerDuty Alerting
import alert

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

# Cấu hình Monitoring
PUSHGATEWAY_URL  = os.getenv("PUSHGATEWAY_URL", "pushgateway:9091")
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_GROUP_ID   = os.getenv("KAFKA_GROUP_ID", "cdc-connect-group")

# Tables to process and their primary keys
TABLE_CONFIG = {
    "orders":        {"pk": ["order_id"]},
    "order_details": {"pk": ["order_id", "product_id"]},
    "products":      {"pk": ["product_id"]},
    "customers":     {"pk": ["customer_id"]},
}

TOPIC_PREFIX = "northwind.public"

# ---------------------------------------------------------------------------
# Prometheus Metrics Setup
# ---------------------------------------------------------------------------
registry = CollectorRegistry()
g_rows     = Gauge('cdc_rows_written_total', 'Deduplicated rows written', ['table'], registry=registry)
g_duration = Gauge('cdc_processing_duration_seconds', 'Processing duration', ['table'], registry=registry)
g_timestamp= Gauge('cdc_last_run_timestamp_seconds', 'Last run timestamp', ['table'], registry=registry)
g_kafka_lag= Gauge('cdc_kafka_consumer_lag', 'Consumer lag per topic/partition', ['table', 'partition'], registry=registry)
g_dq_status= Gauge('cdc_dq_check_status', 'Data quality check status (1=pass, 0=fail)', ['table', 'check'], registry=registry)


def build_spark_session() -> SparkSession:
    """Build and configure a SparkSession with S3A / MinIO support + Deequ."""
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
        # Deequ JAR đã được pre-fetch trong Docker image (/opt/spark/extra-jars/deequ.jar)
        # và truyền qua --jars trong entrypoint.sh → không cần tải từ Maven lúc runtime
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_source_path(table_name: str) -> str:
    """Avro files written by S3 Sink Connector."""
    topic = f"{TOPIC_PREFIX}.{table_name}"
    return f"s3a://{BUCKET}/topics/{topic}/"


def get_sink_path(table_name: str) -> str:
    return f"s3a://{BUCKET}/parquet/{table_name}/"


def collect_kafka_lag():
    """Calculate Kafka lag for the S3 Sink Connector consumer group."""
    logger.info("Collecting Kafka Consumer Lag for group: %s", KAFKA_GROUP_ID)
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            group_id=KAFKA_GROUP_ID,
            enable_auto_commit=False
        )

        for table_name in TABLE_CONFIG.keys():
            topic = f"{TOPIC_PREFIX}.{table_name}"
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                continue

            tps = [TopicPartition(topic, p) for p in partitions]
            end_offsets = consumer.end_offsets(tps)

            for tp in tps:
                committed = consumer.committed(tp)
                end_offset = end_offsets.get(tp, 0)

                # Nếu chưa commit bao giờ, tính lag từ đầu (end_offset)
                lag = max(0, end_offset - (committed or 0))

                g_kafka_lag.labels(table=table_name, partition=str(tp.partition)).set(lag)
                logger.info("Kafka Lag cho %s (partition %d): %d", topic, tp.partition, lag)

        consumer.close()
    except Exception as e:
        logger.warning("Không thể lấy Kafka Lag. Kiểm tra lại Kafka broker. Chi tiết lỗi: %s", e)


@dataclass
class DQResult:
    check_name: str   # vd: "null_pk:order_id"
    passed: bool
    message: str
    error_type: str   # dùng cho PagerDuty dedup_key


def _verification_message(spark: SparkSession, verification_result) -> str:
    """Extract failure message from pydeequ VerificationResult."""
    try:
        result_df = VerificationResult.checkResultsAsDataFrame(spark, verification_result)
        failed_rows = (
            result_df.filter(F.col("constraint_status") != "Success")
            .select("constraint", "constraint_message")
            .collect()
        )
        if not failed_rows:
            return "all constraints passed"
        details = []
        for row in failed_rows:
            constraint = row["constraint"] or "unknown_constraint"
            message = row["constraint_message"] or "no failure detail"
            details.append(f"{constraint}: {message}")
        return "; ".join(details)
    except Exception as exc:
        return f"unable to parse pydeequ result details: {exc}"


def _run_pydeequ_check(
    spark: SparkSession,
    df: DataFrame,
    check_name: str,
    error_type: str,
    check: Check,
) -> DQResult:
    """Run a single pydeequ check and normalize to DQResult."""
    verification_result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(check)
        .run()
    )
    passed = str(verification_result.status) == "Success"
    return DQResult(
        check_name=check_name,
        passed=passed,
        message="all constraints passed" if passed else _verification_message(spark, verification_result),
        error_type=error_type,
    )


def run_dq_checks(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    pk_cols: List[str],
) -> bool:
    """
    Amazon Deequ (pydeequ) data quality checks.

    Checks thực hiện cho mỗi table:
      1. Non-empty       — hasSize: >= 1 row
      2. Null PK         — isComplete: không có PK nào NULL
      3. Uniqueness PK   — hasUniqueness: không có PK trùng lặp (hỗ trợ composite key)
      4. Valid cdc_op    — isContainedIn: cdc_op ∈ {c, u, r}

    Returns:
        True nếu TẤT CẢ checks pass, False nếu có bất kỳ check nào fail.
    """

    logger.info("[DQ] Running data quality checks for table '%s'...", table_name)
    results: List[DQResult] = []

    # ── Check 1: Non-empty ────────────────────────────────────────────────────
    non_empty_check = (
        Check(spark, CheckLevel.Error, f"{table_name}:non_empty")
        .hasSize(lambda size: size >= 1)
    )
    results.append(_run_pydeequ_check(spark, df, "non_empty", "empty_table", non_empty_check))

    # ── Checks 2: Null PK per column ──────────────────────────────────────────
    for pk_col in pk_cols:
        null_pk_check = (
            Check(spark, CheckLevel.Error, f"{table_name}:null_pk:{pk_col}")
            .isComplete(pk_col)
        )
        results.append(_run_pydeequ_check(spark, df, f"null_pk:{pk_col}", "null_pk", null_pk_check))

    # ── Check 3: Uniqueness PK (composite key aware) ─────────────────────────
    unique_pk_check = (
        Check(spark, CheckLevel.Error, f"{table_name}:unique_pk")
        .hasUniqueness(pk_cols, lambda value: value == 1.0)
    )
    unique_label = "unique_pk:" + "+".join(pk_cols)
    results.append(_run_pydeequ_check(spark, df, unique_label, "duplicate_pk", unique_pk_check))

    # ── Check 4: Valid cdc_op ─────────────────────────────────────────────────
    if "cdc_op" in df.columns:
        valid_cdc_check = (
            Check(spark, CheckLevel.Error, f"{table_name}:valid_cdc_op")
            .isContainedIn("cdc_op", ["c", "u", "r"])
        )
        results.append(_run_pydeequ_check(spark, df, "valid_cdc_op", "invalid_cdc_op", valid_cdc_check))

    # ── Tổng hợp & emit Prometheus + PagerDuty ───────────────────────────────
    all_passed = True
    for r in results:
        g_dq_status.labels(table=table_name, check=r.check_name).set(1 if r.passed else 0)
        if r.passed:
            logger.info("[DQ] ✓ PASS | table=%s | check=%s | %s",
                        table_name, r.check_name, r.message)
        else:
            all_passed = False
            logger.error("[DQ] ✗ FAIL | table=%s | check=%s | %s",
                         table_name, r.check_name, r.message)
            alert.send_alert(
                summary=f"[CDC DQ] {table_name}: {r.check_name} FAILED",
                severity="critical",
                table=table_name,
                error_type=r.error_type,
                details={"check": r.check_name, "message": r.message},
            )

    if all_passed:
        logger.info("[DQ] ✓ ALL CHECKS PASSED for table '%s'", table_name)
    else:
        logger.error("[DQ] ✗ SOME CHECKS FAILED for table '%s' — see above.", table_name)
    return all_passed


def _classify_check_error(check_name: str, pk_cols: List[str]) -> str:
    """Map tên check → error_type string cho PagerDuty dedup_key."""
    name_lower = check_name.lower()
    if "null_pk" in name_lower:
        return "null_pk"
    if "unique_pk" in name_lower:
        return "duplicate_pk"
    if "cdc_op" in name_lower:
        return "invalid_cdc_op"
    if "non_empty" in name_lower:
        return "empty_table"
    return "dq_check_failed"


def process_table(spark: SparkSession, table_name: str, pk_cols: List[str]) -> None:
    """
    Read Avro files for a single table, extract latest row per PK,
    add partition columns, and write partitioned Parquet for Trino to query.
    After writing, run Deequ data quality checks and alert on failure.
    """
    source_path = get_source_path(table_name)
    sink_path   = get_sink_path(table_name)

    logger.info("Processing table '%s' from %s", table_name, source_path)

    start_time   = time.time()
    rows_written = 0

    try:
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
        )

        rows_written = df_deduped.count()
        logger.info("Deduplicated record count for '%s': %d", table_name, rows_written)

        # ------------------------------------------------------------------
        # 4. Add partition columns year / month / day
        # ------------------------------------------------------------------
        now_utc = datetime.now(timezone.utc)
        df_partitioned = df_deduped.withColumns({
            "year":  F.lit(str(now_utc.year)),
            "month": F.lit(str(now_utc.month).zfill(2)),
            "day":   F.lit(str(now_utc.day).zfill(2)),
        })

        # ------------------------------------------------------------------
        # 5. Write Parquet — partitioned by year/month/day, Snappy compressed
        # ------------------------------------------------------------------
        (
            df_partitioned
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .partitionBy("year", "month", "day")
            .parquet(sink_path)
        )

        logger.info(
            "Wrote Parquet for '%s' → %s  (partition: year=%s/month=%s/day=%s)",
            table_name, sink_path,
            now_utc.year, str(now_utc.month).zfill(2), str(now_utc.day).zfill(2),
        )

        # ------------------------------------------------------------------
        # 6. [NEW] Deequ Data Quality Checks — chạy sau khi write Parquet
        #    Dùng df_deduped (bỏ partition cols để check sạch hơn)
        # ------------------------------------------------------------------
        run_dq_checks(spark, df_deduped, table_name, pk_cols)

    finally:
        # Cập nhật Metrics dù thành công hay rỗng (để Grafana không bị đứt quãng dữ liệu)
        duration = time.time() - start_time
        g_rows.labels(table=table_name).set(rows_written)
        g_duration.labels(table=table_name).set(duration)
        g_timestamp.labels(table=table_name).set(time.time())


def main() -> None:
    logger.info("=" * 60)
    logger.info("CDC Processor starting  (Avro → Parquet)")
    logger.info("Source bucket : %s", BUCKET)
    logger.info("MinIO endpoint: %s", MINIO_ENDPOINT)
    logger.info("Sink prefix   : parquet/")
    logger.info("PagerDuty key : %s", "configured" if alert.PAGERDUTY_ROUTING_KEY else "NOT SET (graceful degradation)")
    logger.info("=" * 60)

    # Đo độ trễ Kafka trước khi bắt đầu xử lý S3
    collect_kafka_lag()

    spark = build_spark_session()

    for table_name, cfg in TABLE_CONFIG.items():
        try:
            process_table(spark, table_name, cfg["pk"])
        except Exception as e:
            logger.error(
                "Unhandled error processing table '%s': %s",
                table_name, e, exc_info=True,
            )
            # Gửi PagerDuty alert cho unhandled exception
            alert.send_alert(
                summary=f"[CDC Pipeline] Unhandled exception in table '{table_name}'",
                severity="critical",
                table=table_name,
                error_type="unhandled_exception",
                details={"exception": str(e)},
            )

    spark.stop()

    # Đẩy toàn bộ dữ liệu đo đạc (Metrics) sang Pushgateway
    try:
        push_to_gateway(PUSHGATEWAY_URL, job='cdc_processor', registry=registry)
        logger.info("Successfully pushed metrics to Pushgateway: %s", PUSHGATEWAY_URL)
    except Exception as e:
        logger.error("Failed to push metrics to Pushgateway: %s", e)

    logger.info("CDC Processor run complete.")


if __name__ == "__main__":
    main()