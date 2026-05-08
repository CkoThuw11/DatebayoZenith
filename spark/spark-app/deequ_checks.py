import time
import logging
from dataclasses import dataclass
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from prometheus_client import Gauge

import alert

logger = logging.getLogger("dq_checks")


def _dq_severity(error_type: str) -> str:
    """
    Map error_type to PagerDuty severity.
    null_pk / duplicate_pk corrupt the table — critical.
    invalid_cdc_op / stale_data are serious but recoverable — error.
    """
    return "critical" if error_type in ("null_pk", "duplicate_pk") else "error"

@dataclass
class DQResult:
    check_name: str  
    passed: bool
    message: str
    error_type: str



def _verification_message(spark: SparkSession, verification_result) -> str:
    import re
    try:
        result_df = VerificationResult.checkResultsAsDataFrame(spark, verification_result)
        failed_rows = (
            result_df.filter(F.col("constraint_status") != "Success")
            .select("constraint", "constraint_message")
            .collect()
        )
        if not failed_rows:
            return "all constraints passed"

        messages = []
        for row in failed_rows:
            constraint = row["constraint"] or "unknown"
            message    = row["constraint_message"] or "no detail"

            inner = re.search(r'\((\w+)', constraint)
            clean_name = inner.group(1) if inner else constraint

            if "Empty state for analyzer" in message:
                message = "no rows to evaluate"

            messages.append(f"{clean_name}: {message}")

        return "; ".join(messages)
    except Exception as exc:
        return f"unable to parse pydeequ result: {exc}"


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
    g_dq_status: Gauge,
    freshness_threshold_seconds: int,
    consecutive_failures: int,        
    dashboard_url: str,     
) -> bool:
    """
    Run all DQ checks on df (deduped, pre-write).

    Checks:
      1. Non-empty         — at least 1 row (early exit on failure — remaining checks skipped)
      2. Null PK           — no NULL in any PK column
      3. Unique PK         — no duplicate PK (composite-key aware)
      4. Valid cdc_op      — cdc_op ∈ {c, u, r}
      5. Freshness         — latest cdc_ts_ms within freshness_threshold_seconds

    Args:
        spark:        active SparkSession
        df:           deduped DataFrame (business columns only, no partition cols)
        table_name:   used for logging and alert labeling
        pk_cols:      list of primary key column names
        g_dq_status:  Prometheus Gauge for emitting per-check pass/fail

    Returns:
        True if ALL checks pass, False otherwise.
    """
    logger.info("[DQ] Running checks for table '%s'", table_name)
    results: List[DQResult] = []

    # ── Check 1: Non-empty — early exit if table is empty ────────────────
    non_empty_result = _run_pydeequ_check(
        spark, df,
        check_name="non_empty",
        error_type="empty_table",
        check=Check(spark, CheckLevel.Error, f"{table_name}:non_empty")
             .hasSize(lambda size: size >= 1),
    )
    g_dq_status.labels(table=table_name, check="non_empty").set(1 if non_empty_result.passed else 0)
    if not non_empty_result.passed:
        logger.error("[DQ] ✗ FAIL | table=%s | check=non_empty | table has 0 rows — skipping remaining checks", table_name)
        alert.send_alert(
            summary=f"[CDC DQ] '{table_name}': non_empty FAILED — table has 0 rows",
            severity="error",
            table=table_name,
            error_type="empty_table",
            group="data-quality",
            component="spark/dq_checks",
            details={
                "consecutive_failures": consecutive_failures,
                "check":                "non_empty",
                "message":              "table has 0 rows",
                "dashboard_url":        dashboard_url,
            },
        )
        return False

    # ── Check 2: Null PK per column ───────────────────────────────────────
    for pk_col in pk_cols:
        results.append(_run_pydeequ_check(
            spark, df,
            check_name=f"null_pk:{pk_col}",
            error_type="null_pk",
            check=Check(spark, CheckLevel.Error, f"{table_name}:null_pk:{pk_col}")
                 .isComplete(pk_col),
        ))

    # ── Check 3: Unique PK (composite-key aware) ──────────────────────────
    results.append(_run_pydeequ_check(
        spark, df,
        check_name="unique_pk:" + "+".join(pk_cols),
        error_type="duplicate_pk",
        check=Check(spark, CheckLevel.Error, f"{table_name}:unique_pk")
             .hasUniqueness(pk_cols, lambda value: value == 1.0),
    ))

    # ── Check 4: Valid cdc_op ─────────────────────────────────────────────
    if "cdc_op" in df.columns:
        results.append(_run_pydeequ_check(
            spark, df,
            check_name="valid_cdc_op",
            error_type="invalid_cdc_op",
            check=Check(spark, CheckLevel.Error, f"{table_name}:valid_cdc_op")
                 .isContainedIn("cdc_op", ["c", "u", "r"]),
        ))

    # ── Check 5: Freshness (manual — not pydeequ) ─────────────────────────
    if "cdc_ts_ms" in df.columns:
        try:
            now_ms = int(time.time() * 1000)
            max_ts = df.agg(F.max("cdc_ts_ms")).collect()[0][0]
            if max_ts is not None:
                latency_seconds = (now_ms - max_ts) / 1000.0
                passed = latency_seconds <= freshness_threshold_seconds
                results.append(DQResult(
                    check_name="freshness",
                    passed=passed,
                    message=f"latest event is {latency_seconds:.1f} seconds old (threshold: {freshness_threshold_seconds} seconds)",
                    error_type="stale_data",
                ))
        except Exception as e:
            logger.warning("[DQ] Could not compute freshness for '%s': %s", table_name, e)

    all_passed = True
    for r in results:
        g_dq_status.labels(table=table_name, check=r.check_name).set(1 if r.passed else 0)
        if r.passed:
            logger.info("[DQ] ✓ PASS | table=%s | check=%s | %s", table_name, r.check_name, r.message)
        else:
            all_passed = False
            logger.error("[DQ] ✗ FAIL | table=%s | check=%s | %s", table_name, r.check_name, r.message)
            alert.send_alert(
                summary=f"[CDC DQ] '{table_name}': {r.check_name} FAILED — {r.message}",
                severity=_dq_severity(r.error_type),
                table=table_name,
                error_type=r.error_type,
                group="data-quality",
                component="spark/dq_checks",
                details={
                    "consecutive_failures": consecutive_failures,
                    "check":                r.check_name,
                    "message":              r.message,
                    "dashboard_url":        dashboard_url,
                },
            )

    if all_passed:
        logger.info("[DQ] ✓ ALL CHECKS PASSED for table '%s'", table_name)
    else:
        logger.error("[DQ] ✗ SOME CHECKS FAILED for table '%s'", table_name)

    return all_passed