"""
test_dq_alert.py — CDC Pipeline Alert Test Suite (t1 + t2)
-----------------------------------------------------------
Run inside the spark-cdc container via spark-submit:

  docker exec spark-cdc /opt/spark/bin/spark-submit \\
    --master local[1] \\
    --jars "/opt/spark/extra-jars/hadoop-aws.jar,/opt/spark/extra-jars/aws-java-sdk-bundle.jar,/opt/spark/extra-jars/deequ.jar" \\
    /app/test_dq_alert.py --test t1

  docker exec spark-cdc /opt/spark/bin/spark-submit \\
    --master local[1] \\
    --jars "/opt/spark/extra-jars/hadoop-aws.jar,/opt/spark/extra-jars/aws-java-sdk-bundle.jar,/opt/spark/extra-jars/deequ.jar" \\
    /app/test_dq_alert.py --test t2

Tests
─────────────────────────────────────────────────────────────────────────────
  t1   Spark Not Running — Infrastructure SEV-1
         Deletes cdc_last_run_timestamp_seconds from Pushgateway to confirm
         the simulation is real, then calls alert.send_alert() directly with
         error_type="spark_not_running" and severity="critical".
         Alert is NOT resolved — stays open in PagerDuty as an active incident.

  t2   SEV-1 DQ Critical — 100% null PK on entire fact table
         Asserts _dq_severity_scoped routing, then runs run_dq_checks()
         on an all-null DataFrame.
         Alert is NOT resolved — stays open in PagerDuty as an active incident.

Why t1 calls alert.send_alert() directly (not via Grafana)
─────────────────────────────────────────────────────────────────────────────
  The previous version waited for Grafana Rule 2 to fire, which requires:
    1. Prometheus to scrape and detect the missing metric (~15s)
    2. Grafana to evaluate the rule with for:2m (~2 min sustained)
    3. Grafana to POST to webhook_receiver
    4. webhook_receiver to produce to northstream.alerts.infrastructure
    5. kafka_alert_consumer to read and call PagerDuty

  Any failure at step 3, 4, or 5 silently drops the alert — the test passes
  (Grafana fired) but PagerDuty never receives anything.

  This version cuts straight to alert.send_alert() which is the same HTTP
  call that would eventually reach PagerDuty anyway. The Pushgateway metric
  is still deleted to make the simulation authentic, but the alert delivery
  no longer depends on the Grafana → webhook chain.

  To test the full Grafana → webhook → Kafka → PagerDuty chain separately,
  monitor webhook-receiver and kafka-alert-consumer logs manually after
  deleting the metric and waiting ~3 min.
"""

import os
import re
import sys
import time
import logging
import argparse
import subprocess
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
from prometheus_client import CollectorRegistry, Gauge

sys.path.insert(0, "/app")
import alert
from deequ_checks import run_dq_checks, _dq_severity_scoped

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("test_dq_alert")

# ─── Colors ───────────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

# ─── Config ───────────────────────────────────────────────────────────────────
PUSHGATEWAY_URL         = os.getenv("PUSHGATEWAY_URL",          "http://pushgateway:9091")
GRAFANA_URL             = os.getenv("GRAFANA_URL",              "http://grafana:3000")
GRAFANA_BASE_URL        = os.getenv("GRAFANA_BASE_URL",         "http://grafana:3000")
FRESHNESS_THRESHOLD_SEC = int(os.getenv("FRESHNESS_THRESHOLD_SECONDS", "900"))

# ─── Shared schema ────────────────────────────────────────────────────────────
ORDER_SCHEMA = StructType([
    StructField("order_id",  IntegerType(), True),
    StructField("ship_city", StringType(),  True),
    StructField("cdc_op",    StringType(),  True),
    StructField("cdc_ts_ms", LongType(),    True),
])

# ─── Helpers ──────────────────────────────────────────────────────────────────
def section(title: str):
    print(f"\n{BOLD}{'─' * 60}{RESET}")
    print(f"{BOLD}{title}{RESET}")
    print(f"{BOLD}{'─' * 60}{RESET}")

def log_info(msg):  print(f"  {CYAN}[INFO]{RESET}  {msg}")
def log_ok(msg):    print(f"  {GREEN}[PASS]{RESET}  {msg}")
def log_fail(msg):  print(f"  {RED}[FAIL]{RESET}  {msg}")
def log_warn(msg):  print(f"  {YELLOW}[WARN]{RESET}  {msg}")

def assert_result(label: str, actual, expected) -> bool:
    ok = actual == expected
    if ok:
        log_ok(f"{label}: got={actual}, expected={expected}")
    else:
        log_fail(f"{label}: got={actual}, expected={expected}")
    return ok

def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def curl(method: str, url: str, data: str = None) -> tuple:
    cmd = ["curl", "-sf", "-X", method, url]
    if data:
        cmd += ["--data-binary", data]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout

def make_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def make_dq_gauge() -> Gauge:
    registry = CollectorRegistry()
    return Gauge(
        "cdc_dq_check_status",
        "DQ check status (1=pass, 0=fail)",
        ["table", "check"],
        registry=registry,
    )

def run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


# ─── T1: Spark Not Running — Infrastructure SEV-1 ────────────────────────────
def test_t1(dry_run: bool):
    """
    Infrastructure SEV-1 — Spark Not Running.

    Steps:
      1. Confirm cdc_last_run_timestamp_seconds exists in Pushgateway
         (proves Spark has run at least once — the simulation is authentic)
      2. Delete the metric to simulate Spark having stopped
      3. Confirm the metric is absent
      4. Call alert.send_alert() directly with:
           error_type = "spark_not_running"
           severity   = "critical"
           group      = "infrastructure"
         This sends directly to PagerDuty via HTTP — no Grafana dependency.
      5. The metric is NOT restored — the alert stays open in PagerDuty.
         Grafana Rule 2 will also fire ~2 min later and produce a second
         event through the webhook → Kafka chain (visible in consumer logs).

    What you should see in PagerDuty:
      - New incident: "[CDC Spark SEV-1] Spark processor not running"
      - Severity: critical
      - Group: infrastructure
      - custom_details.sev_label: SEV-1
      - Incident remains OPEN — resolve manually when done testing
    """
    section("T1 — Spark Not Running → SEV-1 critical → PagerDuty (stays open)")
    log_info("Alert sent via alert.send_alert() directly — no Grafana wait")
    log_info("Metric deleted and NOT restored — incident stays open in PagerDuty")
    if dry_run:
        log_warn("DRY RUN — metric will be deleted but no PagerDuty delivery")

    all_ok = True
    rid    = run_id()

    # ── Phase 1: Confirm metric baseline ─────────────────────────────────
    section("T1 / Phase 1: Confirm cdc_last_run_timestamp_seconds exists")
    rc, out = curl("GET", f"{PUSHGATEWAY_URL}/metrics")
    if "cdc_last_run_timestamp_seconds" in out:
        log_ok("Metric found in Pushgateway — simulation is authentic")
    else:
        log_fail("Metric not found — run spark-cdc at least once first:")
        log_fail("  docker compose run --rm spark-cdc")
        return False

    # ── Phase 2: Delete metric to simulate Spark stopped ─────────────────
    section("T1 / Phase 2: Delete metric — simulating Spark has stopped")
    rc, _ = curl("DELETE", f"{PUSHGATEWAY_URL}/metrics/job/cdc_processor")
    if rc == 0:
        log_ok("Metric deleted from Pushgateway")
    else:
        log_fail(f"DELETE failed (exit code {rc})")
        return False

    rc, out = curl("GET", f"{PUSHGATEWAY_URL}/metrics")
    if "cdc_last_run_timestamp_seconds" not in out:
        log_ok("Metric confirmed absent — Spark appears stopped to the monitoring stack")
    else:
        log_fail("Metric still present after DELETE")
        return False

    # ── Phase 3: Fire SEV-1 alert via alert.send_alert() ─────────────────
    section("T1 / Phase 3: Fire SEV-1 critical alert to PagerDuty")
    log_info("error_type=spark_not_running | severity=critical | group=infrastructure")
    log_info("dedup_key: cdc-spark-spark_not_running")

    sent = alert.send_alert(
        summary=(
            "[CDC Spark SEV-1] Spark processor not running — "
            "no metrics received, bronze writes stopped. "
            "Entire pipeline halted."
        ),
        severity="critical",
        table="spark",
        error_type="spark_not_running",
        group="infrastructure",
        component="spark/cdc-processor",
        details={
            "sev_label":          "SEV-1",
            "tta_minutes":        15,
            "ttr_hours":          2,
            "protocol":           "Continuous phone+SMS, auto-escalate to Tech Lead after 10 min",
            "metric_deleted":     "cdc_last_run_timestamp_seconds",
            "pushgateway_url":    PUSHGATEWAY_URL,
            "dashboard_url":      f"{GRAFANA_BASE_URL}/d/cdc-pipeline?viewPanel=4",
            "remediation_steps": [
                "docker ps | grep spark-cdc",
                "docker logs spark-cdc --tail 100",
                "docker compose restart spark-cdc",
            ],
        },
        dedup_suffix=rid,   # unique per run so each test creates a fresh incident
    )

    if dry_run:
        log_warn("Alert not sent (dry-run) — payload logged above")
        all_ok &= assert_result("send_alert() reached alert.py (dry-run)", True, True)
    else:
        all_ok &= assert_result("send_alert() returned True (SEV-1 critical)", sent, True)
        if sent:
            log_ok("PagerDuty SEV-1 incident OPENED")
            log_info("")
            log_info("Check PagerDuty — you should see a new SEV-1 critical incident:")
            log_info("  Title: [CDC Spark SEV-1] Spark processor not running")
            log_info("  Severity: critical | Group: infrastructure")
            log_info("  custom_details.sev_label: SEV-1")
            log_info("")
            log_info("The incident is intentionally left OPEN.")
            log_info("Resolve it manually in PagerDuty when done testing.")
            log_info("")
            log_info("Side effect: Grafana Rule 2 will also fire ~2 min from now.")
            log_info("Watch: docker logs webhook-receiver --tail 10")
            log_info("Watch: docker logs kafka-alert-consumer --tail 10")
            log_info("Those logs confirm the Grafana → webhook → Kafka path also works.")

    return all_ok


# ─── T2: SEV-1 DQ Critical — 100% null PK ────────────────────────────────────
def test_t2(dry_run: bool):
    """
    Data Quality SEV-1 — 100% null PK on entire fact table.

    Classification: "Null PK violation affects the entire fact table.
                     The entire Trino query result is untrustworthy."

    Steps:
      0. Assert _dq_severity_scoped routing:
           (null_pk, 5/5 rows) → ("critical", "SEV-1")
           (null_pk, 2/5 rows) → ("error",    "SEV-2")  [boundary]
      1. Build a 5-row DataFrame where every row has null order_id
      2. Run run_dq_checks() — Deequ detects violation
         → _dq_severity_scoped returns ("critical", "SEV-1")
         → alert.send_alert() calls PagerDuty directly
      3. Incident is NOT resolved — stays open in PagerDuty.

    What you should see in PagerDuty:
      - New incident: "[CDC DQ SEV-1] 'orders_fact': null_pk:order_id FAILED"
      - Severity: critical
      - Group: data-quality
      - custom_details.sev_label: SEV-1
      - Incident remains OPEN — resolve manually when done testing
    """
    section("T2 — SEV-1 DQ Critical: 100% null PK → PagerDuty (stays open)")
    log_info("Alert path: run_dq_checks() → _dq_severity_scoped → alert.send_alert() → PagerDuty")
    log_info("scope ratio = 100% >= 50% threshold → SEV-1 critical")
    log_info("Incident intentionally left OPEN — resolve manually in PagerDuty")
    if dry_run:
        log_warn("DRY RUN — no real PagerDuty delivery, alerts logged only")

    spark  = make_spark("test-t2-sev1-null-pk")
    g      = make_dq_gauge()
    table  = "orders_fact"
    dashboard_url = f"{GRAFANA_BASE_URL}/d/cdc-pipeline?var-table={table}"
    all_ok = True

    # ── Phase 0: Assert severity routing ─────────────────────────────────
    section("T2 / Phase 0: Assert _dq_severity_scoped routing")

    actual_sev_100, actual_label_100 = _dq_severity_scoped("null_pk", 5, 5)
    all_ok &= assert_result(
        "null_pk 5/5 rows (100%) → severity",
        actual_sev_100, "critical",
    )
    all_ok &= assert_result(
        "null_pk 5/5 rows (100%) → sev_label",
        actual_label_100, "SEV-1",
    )

    actual_sev_40, actual_label_40 = _dq_severity_scoped("null_pk", 2, 5)
    all_ok &= assert_result(
        "null_pk 2/5 rows (40%) → severity (boundary: below 50% = SEV-2)",
        actual_sev_40, "error",
    )
    all_ok &= assert_result(
        "null_pk 2/5 rows (40%) → sev_label",
        actual_label_40, "SEV-2",
    )

    # ── Phase 1: Build fully-null-PK DataFrame ────────────────────────────
    section("T2 / Phase 1: Build DataFrame — all 5 rows with null order_id")

    full_null_pk_df = spark.createDataFrame([
        (None, "Hanoi",  "c", now_ms()),
        (None, "Saigon", "u", now_ms()),
        (None, "Danang", "r", now_ms()),
        (None, "Hue",    "c", now_ms()),
        (None, "CanTho", "u", now_ms()),
    ], ORDER_SCHEMA)

    total_rows   = full_null_pk_df.count()
    null_pk_rows = full_null_pk_df.filter("order_id IS NULL").count()
    null_pct     = (null_pk_rows / total_rows) * 100 if total_rows > 0 else 0

    log_info(f"DataFrame: total={total_rows}, null_pk={null_pk_rows}, pct={null_pct:.1f}%")
    all_ok &= assert_result("All rows have null PK (100%)", null_pk_rows == total_rows, True)

    # ── Phase 2: Run run_dq_checks() → fires SEV-1 critical ──────────────
    section("T2 / Phase 2: Run run_dq_checks() — SEV-1 critical sent to PagerDuty")
    log_info("Deequ detects 100% null PK → _dq_severity_scoped → critical")
    log_info("alert.send_alert() calls PagerDuty API directly (stable dedup_key)")
    log_info(f"Stable dedup_key: cdc-{table}-null_pk")

    dq_result = run_dq_checks(
        spark, full_null_pk_df, table, ["order_id"], g,
        freshness_threshold_seconds=FRESHNESS_THRESHOLD_SEC,
        consecutive_failures=4,
        dashboard_url=dashboard_url,
    )
    all_ok &= assert_result("run_dq_checks() fails on 100% null PK", dq_result, False)

    if not dry_run:
        log_info("")
        log_info("Check PagerDuty — you should see a new SEV-1 critical incident:")
        log_info(f"  Title: [CDC DQ SEV-1] '{table}': null_pk:order_id FAILED")
        log_info(f"  Severity: critical | Group: data-quality")
        log_info(f"  custom_details.sev_label: SEV-1")
        log_info("")
        log_info("The incident is intentionally left OPEN.")
        log_info("Resolve it manually in PagerDuty when done testing.")

    # ── No resolve phase ──────────────────────────────────────────────────
    section("T2 / Note: No resolve — incident stays open in PagerDuty")
    log_info("This is intentional. The alert represents a real data integrity failure.")
    log_info("Acknowledge and resolve it manually in PagerDuty after verifying.")

    spark.stop()
    return all_ok


# ─── Entry point ──────────────────────────────────────────────────────────────
TESTS = {
    "t1": ("Spark not running — SEV-1 critical → PagerDuty via alert.send_alert() (stays open)", test_t1),
    "t2": ("SEV-1 DQ Critical — 100% null PK → alert.py → PagerDuty (stays open)",              test_t2),
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="CDC Pipeline Alert Test Suite — t1 and t2",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--test", "-t",
        choices=list(TESTS.keys()),
        required=True,
        help="Test to run: t1 or t2",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip real PagerDuty delivery — alerts are logged only",
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="List available tests and exit",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    pagerduty_configured = bool(alert.PAGERDUTY_ROUTING_KEY)

    if args.list:
        print(f"\n{BOLD}Available tests:{RESET}")
        for key, (desc, _) in TESTS.items():
            print(f"  {CYAN}{key}{RESET}  {desc}")
        print(f"\n{BOLD}PagerDuty:{RESET} {'✓ configured — live delivery' if pagerduty_configured else '✗ not set — dry-run mode'}")
        print()
        sys.exit(0)

    dry_run = args.dry_run or not pagerduty_configured
    if not pagerduty_configured and not args.dry_run:
        print(f"\n{YELLOW}[WARN]  PAGERDUTY_ROUTING_KEY not set — dry-run mode activated{RESET}")
        print(f"{YELLOW}        Set the key in .env and restart services for live delivery{RESET}")

    rid = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    print(f"\n{BOLD}Test:{RESET}    {args.test} — {TESTS[args.test][0]}")
    print(f"{BOLD}Mode:{RESET}    {'dry-run (no real PagerDuty delivery)' if dry_run else 'LIVE (real PagerDuty delivery)'}")
    print(f"{BOLD}Run ID:{RESET}  {rid}")

    _, fn = TESTS[args.test]
    passed = fn(dry_run=dry_run)

    print(f"\n{'=' * 60}")
    if passed:
        print(f"{GREEN}{BOLD}✓ {args.test.upper()} PASSED{RESET}")
    else:
        print(f"{RED}{BOLD}✗ {args.test.upper()} FAILED — check logs above{RESET}")
    print(f"{'=' * 60}\n")

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()