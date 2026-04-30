"""
test_dq_alert.py — End-to-end test cho Data Quality + PagerDuty Alerting
------------------------------------------------------------------------
Chạy bên trong spark-cdc container:

  docker exec spark-cdc /opt/spark/bin/spark-submit \\
    --master local[1] \\
    --jars "/opt/spark/extra-jars/hadoop-aws.jar,/opt/spark/extra-jars/aws-java-sdk-bundle.jar,/opt/spark/extra-jars/deequ.jar" \\
    /app/test_dq_alert.py

Test này dùng table suffix theo ngày/giờ để incident hiển thị "mới" cho mỗi run.
"""

import os
import sys
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType,
)

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

# ─── Schema chuẩn dùng chung ──────────────────────────────────────────────────
ORDER_SCHEMA = StructType([
    StructField("order_id",  IntegerType(), True),
    StructField("ship_city", StringType(),  True),
    StructField("cdc_op",    StringType(),  True),
])

# ─── Helpers ──────────────────────────────────────────────────────────────────

def print_scenario(n: int, title: str):
    print(f"\n{BOLD}{'─'*60}{RESET}")
    print(f"{BOLD}SCENARIO {n}: {title}{RESET}")
    print(f"{BOLD}{'─'*60}{RESET}")


def assert_result(label: str, actual: bool, expected: bool) -> bool:
    ok     = actual == expected
    status = "PASS" if ok else "FAIL"
    color  = GREEN  if ok else RED
    print(f"  {color}[{status}]{RESET} {label}: got={actual}, expected={expected}")
    return ok


def make_good_rows(spark):
    """3 row hợp lệ."""
    return spark.createDataFrame([
        (1, "Hanoi",   "c"),
        (2, "Saigon",  "u"),
        (3, "Danang",  "r"),
    ], ORDER_SCHEMA)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    spark = (
        SparkSession.builder
        .appName("test-dq-alert")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    pagerduty_configured = bool(alert.PAGERDUTY_ROUTING_KEY)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    print(
        f"\n{BOLD}PagerDuty key:{RESET} "
        f"{'✓ configured — alerts sẽ được gửi thật' if pagerduty_configured else '✗ not set — alerts chỉ log (graceful degradation)'}"
    )
    print(f"{YELLOW}Run ID (UTC): {run_id} — tạo incident mới cho run hiện tại{RESET}")

    all_ok = True

    # ─────────────────────────────────────────────────────────────────────────
    # SCENARIO 1: Good data — tất cả PASS
    # ─────────────────────────────────────────────────────────────────────────
    print_scenario(1, "Good data — expect ALL PASS")

    good_df = make_good_rows(spark)
    result = run_dq_checks(
        spark, good_df, f"orders_test_{run_id}",
        ["order_id"],
    )
    all_ok &= assert_result("All checks pass", result, True)

    # ─────────────────────────────────────────────────────────────────────────
    # SCENARIO 2: NULL PK — phải FAIL + alert
    # ─────────────────────────────────────────────────────────────────────────
    print_scenario(2, "NULL PK — expect FAIL + PagerDuty alert")

    null_pk_df = spark.createDataFrame([
        (None, "Alice",   "c"),
        (2,    "Bob",     "u"),
        (3,    "Charlie", "r"),
    ], ORDER_SCHEMA)

    result = run_dq_checks(
        spark, null_pk_df, f"orders_test_{run_id}",
        ["order_id"],
    )
    all_ok &= assert_result("NULL PK detected → False", result, False)

    # ─────────────────────────────────────────────────────────────────────────
    # SCENARIO 3: Duplicate PK — phải FAIL + alert
    # ─────────────────────────────────────────────────────────────────────────
    print_scenario(3, "Duplicate PK — expect FAIL + PagerDuty alert")

    dup_pk_df = spark.createDataFrame([
        (1, "Alice",   "c"),
        (1, "Bob",     "u"),
        (3, "Charlie", "r"),
    ], ORDER_SCHEMA)

    result = run_dq_checks(
        spark, dup_pk_df, f"orders_test_{run_id}",
        ["order_id"],
    )
    all_ok &= assert_result("Duplicate PK detected → False", result, False)

    # ─────────────────────────────────────────────────────────────────────────
    # SCENARIO 4: Invalid cdc_op ('d') — phải FAIL
    # ─────────────────────────────────────────────────────────────────────────
    print_scenario(4, "Invalid cdc_op ('d') — expect FAIL + PagerDuty alert")

    bad_op_df = spark.createDataFrame([
        (1, "Alice",   "c"),
        (2, "Bob",     "d"),
        (3, "Charlie", "r"),
    ], ORDER_SCHEMA)

    result = run_dq_checks(
        spark, bad_op_df, f"orders_test_{run_id}",
        ["order_id"],
    )
    all_ok &= assert_result("Invalid cdc_op detected → False", result, False)

    # ─────────────────────────────────────────────────────────────────────────
    # SCENARIO 5: Empty table — phải FAIL
    # ─────────────────────────────────────────────────────────────────────────
    print_scenario(5, "Empty table — expect FAIL + PagerDuty alert")

    empty_df = spark.createDataFrame([], ORDER_SCHEMA)

    result = run_dq_checks(
        spark, empty_df, f"orders_test_{run_id}",
        ["order_id"],
    )
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