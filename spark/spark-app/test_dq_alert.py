"""
test_dq_alert.py — CDC Pipeline Alert Test Suite
-------------------------------------------------
Run inside the spark-cdc container via spark-submit:

  # Run a specific test
  docker exec spark-cdc /opt/spark/bin/spark-submit \\
    --master local[1] \\
    --jars "/opt/spark/extra-jars/hadoop-aws.jar,/opt/spark/extra-jars/aws-java-sdk-bundle.jar,/opt/spark/extra-jars/deequ.jar" \\
    /app/test_dq_alert.py --test t1

  # List available tests
  docker exec spark-cdc /opt/spark/bin/spark-submit ... /app/test_dq_alert.py --list

  # Dry-run (no real PagerDuty delivery, alerts logged only)
  docker exec spark-cdc /opt/spark/bin/spark-submit ... /app/test_dq_alert.py --test t1 --dry-run

Tests:
  t1   DQ failure        — null PK, duplicate PK, invalid cdc_op, empty table
  t2   Freshness breach  — fires send_alert() then resolve_alert() for stale_data
  t3   Spark not running — deletes Pushgateway metric, polls Grafana for firing state
"""

import os
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
from deequ_checks import run_dq_checks

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

# ─── Shared schema ─────────────────────────────────────────────────────────────
ORDER_SCHEMA = StructType([
    StructField("order_id",   IntegerType(), True),
    StructField("ship_city",  StringType(),  True),
    StructField("cdc_op",     StringType(),  True),
    StructField("cdc_ts_ms",  LongType(),    True),
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

def assert_result(label: str, actual: bool, expected: bool) -> bool:
    ok = actual == expected
    if ok:
        log_ok(f"{label}: got={actual}, expected={expected}")
    else:
        log_fail(f"{label}: got={actual}, expected={expected}")
    return ok

def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def curl(method: str, url: str, data: str = None) -> tuple:
    """Run a curl command, return (returncode, stdout)."""
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


# ─── T1: DQ Failure ───────────────────────────────────────────────────────────
def test_t1(dry_run: bool):
    """
    Runs 4 DQ failure scenarios through run_dq_checks():
      - Null PK        → send_alert severity=critical
      - Duplicate PK   → send_alert severity=critical
      - Invalid cdc_op → send_alert severity=error
      - Empty table    → send_alert severity=error
    Also confirms good data passes cleanly (no alert fired).
    """
    section("T1 — DQ Failure")
    log_info("Tests run_dq_checks() failure paths + PagerDuty send_alert()")
    if dry_run:
        log_warn("DRY RUN — no real PagerDuty delivery, alerts logged only")

    spark = make_spark("test-t1-dq")
    g     = make_dq_gauge()
    rid   = run_id()
    table = f"orders_test_{rid}"
    dashboard_url = f"{GRAFANA_BASE_URL}/d/cdc-pipeline?var-table=orders"
    all_ok = True

    # Scenario 1: Good data — all checks pass, no alert
    section("T1 / Scenario 1: Good data — expect ALL PASS, no alert")
    good_df = spark.createDataFrame([
        (1, "Hanoi",  "c", now_ms()),
        (2, "Saigon", "u", now_ms()),
        (3, "Danang", "r", now_ms()),
    ], ORDER_SCHEMA)
    result = run_dq_checks(
        spark, good_df, table, ["order_id"], g,
        freshness_threshold_seconds=FRESHNESS_THRESHOLD_SEC,
        consecutive_failures=0,
        dashboard_url=dashboard_url,
    )
    all_ok &= assert_result("Good data passes", result, True)

    # Scenario 2: Null PK
    section("T1 / Scenario 2: NULL PK — expect FAIL + critical alert")
    null_pk_df = spark.createDataFrame([
        (None, "Alice",   "c", now_ms()),
        (2,    "Bob",     "u", now_ms()),
        (3,    "Charlie", "r", now_ms()),
    ], ORDER_SCHEMA)
    result = run_dq_checks(
        spark, null_pk_df, table, ["order_id"], g,
        freshness_threshold_seconds=FRESHNESS_THRESHOLD_SEC,
        consecutive_failures=1,
        dashboard_url=dashboard_url,
    )
    all_ok &= assert_result("NULL PK → False", result, False)

    # Scenario 3: Duplicate PK
    section("T1 / Scenario 3: Duplicate PK — expect FAIL + critical alert")
    dup_pk_df = spark.createDataFrame([
        (1, "Alice",   "c", now_ms()),
        (1, "Bob",     "u", now_ms()),
        (3, "Charlie", "r", now_ms()),
    ], ORDER_SCHEMA)
    result = run_dq_checks(
        spark, dup_pk_df, table, ["order_id"], g,
        freshness_threshold_seconds=FRESHNESS_THRESHOLD_SEC,
        consecutive_failures=2,
        dashboard_url=dashboard_url,
    )
    all_ok &= assert_result("Duplicate PK → False", result, False)

    # Scenario 4: Invalid cdc_op
    section("T1 / Scenario 4: Invalid cdc_op ('d') — expect FAIL + error alert")
    bad_op_df = spark.createDataFrame([
        (1, "Alice",   "c", now_ms()),
        (2, "Bob",     "d", now_ms()),
        (3, "Charlie", "r", now_ms()),
    ], ORDER_SCHEMA)
    result = run_dq_checks(
        spark, bad_op_df, table, ["order_id"], g,
        freshness_threshold_seconds=FRESHNESS_THRESHOLD_SEC,
        consecutive_failures=0,
        dashboard_url=dashboard_url,
    )
    all_ok &= assert_result("Invalid cdc_op → False", result, False)

    # Scenario 5: Empty table
    section("T1 / Scenario 5: Empty table — expect FAIL + error alert")
    empty_df = spark.createDataFrame([], ORDER_SCHEMA)
    result = run_dq_checks(
        spark, empty_df, table, ["order_id"], g,
        freshness_threshold_seconds=FRESHNESS_THRESHOLD_SEC,
        consecutive_failures=0,
        dashboard_url=dashboard_url,
    )
    all_ok &= assert_result("Empty table → False", result, False)

    spark.stop()
    return all_ok


# ─── T2: Freshness Breach + Resolve ───────────────────────────────────────────
def test_t2(dry_run: bool):
    """
    Calls send_alert() directly with stale_data to simulate a freshness breach,
    then calls resolve_alert() to confirm the incident auto-closes.
    Does not require Spark — purely tests alert.py paths.
    """
    section("T2 — Freshness Breach + Resolve")
    log_info("Directly calls send_alert() then resolve_alert() for stale_data")
    if dry_run:
        log_warn("DRY RUN — no real PagerDuty delivery, alerts logged only")

    all_ok = True
    table  = "orders"
    rid    = run_id()
    fake_gap_s   = FRESHNESS_THRESHOLD_SEC + 180   # 3 min over threshold
    fake_gap_min = fake_gap_s / 60

    # Phase 1: Fire breach alert
    section("T2 / Phase 1: Fire freshness breach alert")
    log_info(f"Simulating {fake_gap_min:.1f} min stale (threshold: {FRESHNESS_THRESHOLD_SEC / 60:.0f} min)")

    sent = alert.send_alert(
        summary=f"[CDC Freshness] '{table}' data is {fake_gap_min:.1f} min stale (threshold: {FRESHNESS_THRESHOLD_SEC / 60:.0f} min)",
        severity="critical",
        table=table,
        error_type="stale_data",
        group="data-quality",
        component="spark/freshness_check",
        details={
            "consecutive_failures":  0,
            "freshness_gap_seconds": round(fake_gap_s, 1),
            "threshold_seconds":     FRESHNESS_THRESHOLD_SEC,
            "dashboard_url":         f"{GRAFANA_BASE_URL}/d/cdc-pipeline?var-table={table}",
        },
        dedup_suffix=rid,   # unique suffix so each test run = fresh incident
    )

    if dry_run:
        log_warn("Alert not sent (dry-run) — check logs above for payload")
        all_ok &= assert_result("send_alert() reached alert.py", True, True)
    else:
        all_ok &= assert_result("send_alert() returned True", sent, True)
        if sent:
            log_ok("PagerDuty incident opened — check your PagerDuty dashboard")

    # Phase 2: Resolve
    section("T2 / Phase 2: Resolve the breach")
    log_info("Simulating recovery — freshness gap back below threshold")
    time.sleep(2)   # small pause so PagerDuty registers the trigger first

    resolved = alert.resolve_alert(
        table=table,
        error_type="stale_data",
        dedup_suffix=rid,   # must match the trigger dedup_suffix
    )

    if dry_run:
        log_warn("Resolve not sent (dry-run) — check logs above")
        all_ok &= assert_result("resolve_alert() reached alert.py", True, True)
    else:
        all_ok &= assert_result("resolve_alert() returned True", resolved, True)
        if resolved:
            log_ok("PagerDuty incident resolved — verify it closed in your dashboard")

    return all_ok


# ─── T3: Spark Not Running (Grafana Rule 2) ────────────────────────────────────
def test_t3(dry_run: bool):
    """
    Deletes cdc_last_run_timestamp_seconds from Pushgateway to simulate
    Spark death, then polls Grafana alerts API until Rule 2 fires.
    Restores by pushing the metric back.
    Does not require Spark.
    """
    section("T3 — Spark Not Running (Grafana Rule 2)")
    log_info("Deletes metric from Pushgateway, polls Grafana for firing state")
    if dry_run:
        log_warn("DRY RUN — metric will be deleted but Grafana polling skipped")

    all_ok = True

    # Phase 1: Confirm baseline
    section("T3 / Phase 1: Baseline — confirm metric exists")
    rc, out = curl("GET", f"{PUSHGATEWAY_URL}/metrics")
    if "cdc_last_run_timestamp_seconds" in out:
        log_ok("cdc_last_run_timestamp_seconds found in Pushgateway")
    else:
        log_fail("Metric not found — run spark-cdc at least once first:")
        log_fail("  docker compose run --rm spark-cdc")
        return False

    # Phase 2: Delete metric
    section("T3 / Phase 2: Delete metric to simulate Spark death")
    rc, _ = curl("DELETE", f"{PUSHGATEWAY_URL}/metrics/job/cdc_processor")
    if rc == 0:
        log_ok("Metric deleted from Pushgateway")
    else:
        log_fail(f"DELETE failed (exit code {rc})")
        return False

    rc, out = curl("GET", f"{PUSHGATEWAY_URL}/metrics")
    if "cdc_last_run_timestamp_seconds" not in out:
        log_ok("Metric confirmed absent")
    else:
        log_fail("Metric still present after DELETE")
        return False

    if dry_run:
        log_warn("DRY RUN — skipping Grafana poll. Restoring metric now.")
        rc, _ = curl(
            "POST",
            f"{PUSHGATEWAY_URL}/metrics/job/cdc_processor",
            f"# TYPE cdc_last_run_timestamp_seconds gauge\ncdc_last_run_timestamp_seconds {int(time.time())}\n",
        )
        log_ok("Metric restored (dry-run)")
        return True

    # Phase 3: Poll Grafana for firing state
    section("T3 / Phase 3: Poll Grafana — waiting for Rule 2 to fire")
    log_info("Prometheus scrapes every 15s + Grafana rule has for: 2m → allow up to 3 min")
    log_info(f"Monitor at: {GRAFANA_URL}/alerting/list")

    fired   = False
    elapsed = 0
    timeout = 180

    while elapsed < timeout:
        rc, response = curl("GET", f"{GRAFANA_URL}/api/prometheus/grafana/api/v1/rules")
        if "firing" in response:
            fired = True
            break
        # Extract state for display
        import re
        states = re.findall(r'"state":"([^"]+)"', response)
        state  = states[0] if states else "unknown"
        log_info(f"Current state: {state} ({elapsed}s elapsed, waiting for 'firing')")
        time.sleep(15)
        elapsed += 15

    all_ok &= assert_result("Grafana Rule 2 fired", fired, True)

    # Phase 4: Restore metric
    section("T3 / Phase 4: Restore metric")
    rc, _ = curl(
        "POST",
        f"{PUSHGATEWAY_URL}/metrics/job/cdc_processor",
        f"# TYPE cdc_last_run_timestamp_seconds gauge\ncdc_last_run_timestamp_seconds {int(time.time())}\n",
    )
    if rc == 0:
        log_ok("Metric restored to Pushgateway")
    else:
        log_warn("Failed to restore metric — run spark-cdc manually to push it back")

    log_info(f"Verify PagerDuty: incident should show component=spark, group=infrastructure")
    return all_ok


# ─── Entry point ──────────────────────────────────────────────────────────────
TESTS = {
    "t1": ("DQ failure — null PK, dup PK, invalid op, empty table",   test_t1),
    "t2": ("Freshness breach + resolve — send_alert then resolve_alert", test_t2),
    "t3": ("Spark not running — delete Pushgateway metric, poll Grafana", test_t3),
}

def parse_args():
    parser = argparse.ArgumentParser(
        description="CDC Pipeline Alert Test Suite",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--test", "-t",
        choices=list(TESTS.keys()),
        help="Test to run: t1, t2, or t3",
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
        print(f"\n{BOLD}PagerDuty:{RESET} {'✓ configured' if pagerduty_configured else '✗ not set (use --dry-run)'}")
        print()
        sys.exit(0)

    if not args.test:
        print(f"{RED}Error: --test is required. Use --list to see available tests.{RESET}")
        sys.exit(1)

    dry_run = args.dry_run or not pagerduty_configured
    if not pagerduty_configured and not args.dry_run:
        print(f"\n{YELLOW}[WARN]  PAGERDUTY_ROUTING_KEY not set — running in dry-run mode automatically{RESET}")

    rid = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    print(f"\n{BOLD}Test:{RESET}    {args.test} — {TESTS[args.test][0]}")
    print(f"{BOLD}Mode:{RESET}    {'dry-run (no real PagerDuty delivery)' if dry_run else 'live (real PagerDuty delivery)'}")
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