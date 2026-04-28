"""
test_dq_alert.py — End-to-end test cho Data Quality + PagerDuty Alerting
------------------------------------------------------------------------
Chạy bên trong spark-cdc container:

  docker exec spark-cdc /opt/spark/bin/spark-submit \\
    --master local[1] \\
    --jars "/opt/spark/extra-jars/hadoop-aws.jar,/opt/spark/extra-jars/aws-java-sdk-bundle.jar" \\
    /app/test_dq_alert.py

Kết quả mong đợi:
  SCENARIO 1 (good data)  → tất cả PASS, không alert
  SCENARIO 2 (null PK)    → null_pk FAIL, PagerDuty alert triggered
  SCENARIO 3 (dup PK)     → unique_pk FAIL, PagerDuty alert triggered
  SCENARIO 4 (bad cdc_op) → valid_cdc_op FAIL, PagerDuty alert triggered
  SCENARIO 5 (empty)      → non_empty FAIL, PagerDuty alert triggered
"""

import os
import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Thêm /app vào path để import alert.py và hàm run_dq_checks
sys.path.insert(0, "/app")
import alert
from cdc_processor import run_dq_checks, DQResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("test_dq_alert")

# ─── Màu sắc terminal ──────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


def print_scenario(n: int, title: str):
    print(f"\n{BOLD}{'─'*60}{RESET}")
    print(f"{BOLD}SCENARIO {n}: {title}{RESET}")
    print(f"{BOLD}{'─'*60}{RESET}")


def assert_result(label: str, actual: bool, expected: bool):
    status = "PASS" if actual == expected else "FAIL"
    color  = GREEN if actual == expected else RED
    print(f"  {color}[{status}]{RESET} {label}: got={actual}, expected={expected}")
    return actual == expected


def main():
    spark = (
        SparkSession.builder
        .appName("test-dq-alert")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")  # tắt noise của Spark, chỉ xem output test

    pagerduty_configured = bool(alert.PAGERDUTY_ROUTING_KEY)
    print(f"\n{BOLD}PagerDuty key:{RESET} {'✓ configured — alerts sẽ được gửi thật' if pagerduty_configured else '✗ not set — alerts chỉ log (graceful degradation)'}")

    all_ok = True

    # ─────────────────────────────────────────────────────────────────────────
    # SCENARIO 1: Good data — tất cả phải PASS
    # ─────────────────────────────────────────────────────────────────────────
    print_scenario(1, "Good data — expect ALL PASS")
    good_df = spark.createDataFrame([
        (1, "Alice",   "c"),
        (2, "Bob",     "u"),
        (3, "Charlie", "r"),
    ], ["order_id", "ship_city", "cdc_op"])

    result = run_dq_checks(spark, good_df, "orders_test", ["order_id"])
    all_ok &= assert_result("All checks pass", result, True)

    # ─────────────────────────────────────────────────────────────────────────
    # SCENARIO 2: NULL PK — phải FAIL + alert
    # ─────────────────────────────────────────────────────────────────────────
    print_scenario(2, "NULL PK — expect FAIL + PagerDuty alert")
    null_pk_df = spark.createDataFrame([
        (None, "Alice",   "c"),   # ← NULL order_id
        (2,    "Bob",     "u"),
        (3,    "Charlie", "r"),
    ], ["order_id", "ship_city", "cdc_op"])

    result = run_dq_checks(spark, null_pk_df, "orders_test", ["order_id"])
    all_ok &= assert_result("NULL PK detected → False", result, False)

    # ─────────────────────────────────────────────────────────────────────────
    # SCENARIO 3: Duplicate PK — phải FAIL + alert
    # ─────────────────────────────────────────────────────────────────────────
    print_scenario(3, "Duplicate PK — expect FAIL + PagerDuty alert")
    dup_pk_df = spark.createDataFrame([
        (1, "Alice",   "c"),
        (1, "Bob",     "u"),   # ← order_id=1 trùng lặp
        (3, "Charlie", "r"),
    ], ["order_id", "ship_city", "cdc_op"])

    result = run_dq_checks(spark, dup_pk_df, "orders_test", ["order_id"])
    all_ok &= assert_result("Duplicate PK detected → False", result, False)

    # ─────────────────────────────────────────────────────────────────────────
    # SCENARIO 4: Invalid cdc_op ('d' = delete không được phép) — phải FAIL
    # ─────────────────────────────────────────────────────────────────────────
    print_scenario(4, "Invalid cdc_op ('d') — expect FAIL + PagerDuty alert")
    bad_op_df = spark.createDataFrame([
        (1, "Alice",   "c"),
        (2, "Bob",     "d"),   # ← 'd' không hợp lệ
        (3, "Charlie", "r"),
    ], ["order_id", "ship_city", "cdc_op"])

    result = run_dq_checks(spark, bad_op_df, "orders_test", ["order_id"])
    all_ok &= assert_result("Invalid cdc_op detected → False", result, False)

    # ─────────────────────────────────────────────────────────────────────────
    # SCENARIO 5: Empty table — phải FAIL
    # ─────────────────────────────────────────────────────────────────────────
    print_scenario(5, "Empty table — expect FAIL + PagerDuty alert")
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    empty_schema = StructType([
        StructField("order_id",  IntegerType(), True),
        StructField("ship_city", StringType(),  True),
        StructField("cdc_op",    StringType(),  True),
    ])
    empty_df = spark.createDataFrame([], empty_schema)

    result = run_dq_checks(spark, empty_df, "orders_test", ["order_id"])
    all_ok &= assert_result("Empty table detected → False", result, False)

    # ─────────────────────────────────────────────────────────────────────────
    # Tổng kết
    # ─────────────────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    if all_ok:
        print(f"{GREEN}{BOLD}✓ ALL SCENARIOS PASSED — DQ checks + alerting hoạt động đúng!{RESET}")
    else:
        print(f"{RED}{BOLD}✗ SOME SCENARIOS FAILED — xem log ở trên để debug.{RESET}")
    print(f"{'='*60}\n")

    spark.stop()
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
