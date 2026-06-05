"""
deequ_checks.py — Data Quality checks for the NorthStream CDC pipeline.

Severity routing (fixed vs original):
─────────────────────────────────────────────────────────────────────────────
  Original _dq_severity() was a flat 2-bucket function:
      null_pk / duplicate_pk → "critical"  (always)
      everything else        → "error"     (always)

  This produced two confirmed bugs (confirmed by gap analysis):

  BUG-1  Partial PK violations (a few bad rows) were escalated to SEV-1
         "critical" instead of SEV-2 "error", because the function had no
         knowledge of how many rows were affected.

  BUG-2  Low-rate invalid_cdc_op violations (< 1% of rows) were escalated
         to SEV-2 "error" instead of SEV-3 "warning", because the function
         had no knowledge of the violation rate.

  BUG-3  stale_data was dispatched as "error" by deequ_checks.py (correct),
         but cdc_processor.py called send_alert() directly with
         severity="critical". That is fixed in cdc_processor.py separately;
         this file only controls the Deequ-layer freshness path.

Fixed design:
─────────────────────────────────────────────────────────────────────────────
  _dq_severity() is replaced by _dq_severity_scoped(), which accepts:
      error_type     : str   — the violation category
      violation_rows : int   — number of rows that failed the check
      total_rows     : int   — total rows in the batch

  Routing table (matches runbook Section 4.2):

  error_type        condition                    severity   SEV label
  ──────────────    ─────────────────────────    ────────   ─────────
  null_pk           violation_ratio >= 0.5       critical   SEV-1
  null_pk           violation_ratio <  0.5       error      SEV-2
  duplicate_pk      violation_ratio >= 0.5       critical   SEV-1
  duplicate_pk      violation_ratio <  0.5       error      SEV-2
  invalid_cdc_op    violation_ratio >= 0.01      error      SEV-2
  invalid_cdc_op    violation_ratio <  0.01      warning    SEV-3
  empty_table       (always)                     error      SEV-2
  stale_data        (always, Deequ layer only)   error      SEV-2 *

  * Infrastructure-layer freshness (Prometheus rule) is SEV-2 "error".
    Data-layer freshness (Deequ, this file) is SEV-3 "warning" per runbook.
    The stale_data path here is the Deequ path → "warning".
    cdc_processor.py sends the infra-layer alert → "error" (fix that file).
"""

import time
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from prometheus_client import Gauge

import alert

logger = logging.getLogger("dq_checks")

# ─── Severity thresholds (matches runbook Section 4.2) ────────────────────────
_PK_SEV1_RATIO        = 0.50   # >= 50% rows affected → SEV-1 critical
_CDC_OP_SEV3_RATIO    = 0.01   # <  1%  rows affected → SEV-3 warning


def _dq_severity_scoped(
    error_type: str,
    violation_rows: int,
    total_rows: int,
) -> Tuple[str, str]:
    """
    Return (pagerduty_severity, sev_label) based on error type and scope.

    Args:
        error_type     : one of null_pk, duplicate_pk, invalid_cdc_op,
                         empty_table, stale_data
        violation_rows : number of rows that triggered the violation
        total_rows     : total rows in the DataFrame being checked

    Returns:
        Tuple of (pd_severity, sev_label) e.g. ("critical", "SEV-1")
    """
    ratio = violation_rows / total_rows if total_rows > 0 else 1.0

    if error_type in ("null_pk", "duplicate_pk"):
        if ratio >= _PK_SEV1_RATIO:
            return ("critical", "SEV-1")   # widespread PK corruption
        return ("error", "SEV-2")          # isolated records, not whole table

    if error_type == "invalid_cdc_op":
        if ratio < _CDC_OP_SEV3_RATIO:
            return ("warning", "SEV-3")    # low rate, Slack-only
        return ("error", "SEV-2")          # significant rate, push notification

    if error_type == "empty_table":
        return ("error", "SEV-2")          # always SEV-2 per runbook 4.2

    if error_type == "stale_data":
        # Deequ-layer freshness → DQ topic → SEV-3 warning (Slack-only)
        # cdc_processor.py infra-layer freshness → infra topic → SEV-2 error
        return ("warning", "SEV-3")

    # fallback for unknown types
    return ("error", "SEV-2")


@dataclass
class DQResult:
    check_name     : str
    passed         : bool
    message        : str
    error_type     : str
    violation_rows : int = 0      # rows that violated the check (0 if passed)
    total_rows     : int = 0      # total rows in the batch at check time


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


def _parse_violation_count_from_message(message: str, total_rows: int) -> int:
    """
    Best-effort extraction of the violation row count from a Deequ message.

    Deequ constraint_message examples:
      "Value: 0.6666666666666666 does not meet the constraint requirement!"
      (for uniqueness / completeness — value is the *passing* fraction)

    For completeness (null check): value = fraction of non-null rows.
      violation_rows ≈ round((1 - value) * total_rows)

    For uniqueness: value = fraction of unique rows.
      violation_rows ≈ round((1 - value) * total_rows)

    For isContainedIn: Deequ reports the fraction of compliant rows.
      violation_rows ≈ round((1 - value) * total_rows)

    Falls back to total_rows on parse failure (safe: worst-case escalation).
    """
    import re
    match = re.search(r'Value:\s*([\d.]+)', message)
    if match:
        try:
            passing_fraction = float(match.group(1))
            violation_fraction = 1.0 - passing_fraction
            return max(1, round(violation_fraction * total_rows))
        except (ValueError, TypeError):
            pass
    # Could not parse — assume worst case so severity is not under-estimated
    return total_rows


def _run_pydeequ_check(
    spark: SparkSession,
    df: DataFrame,
    check_name: str,
    error_type: str,
    check: Check,
    total_rows: int,
) -> DQResult:
    """
    Run a single pydeequ check and normalize to DQResult.

    Now also populates violation_rows and total_rows so that
    _dq_severity_scoped() can route the severity correctly.
    """
    verification_result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(check)
        .run()
    )
    passed  = str(verification_result.status) == "Success"
    message = "all constraints passed" if passed else _verification_message(spark, verification_result)

    violation_rows = 0 if passed else _parse_violation_count_from_message(message, total_rows)

    return DQResult(
        check_name=check_name,
        passed=passed,
        message=message,
        error_type=error_type,
        violation_rows=violation_rows,
        total_rows=total_rows,
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
      1. Non-empty         — at least 1 row (early exit on failure)
      2. Null PK           — no NULL in any PK column
      3. Unique PK         — no duplicate PK (composite-key aware)
      4. Valid cdc_op      — cdc_op ∈ {c, u, r}
      5. Freshness         — latest cdc_ts_ms within freshness_threshold_seconds

    Severity routing (fixed):
      Severity is now scope-aware — see _dq_severity_scoped() docstring.

    Returns:
      True if ALL checks pass, False otherwise.
    """
    logger.info("[DQ] Running checks for table '%s'", table_name)
    results: List[DQResult] = []

    total_rows = df.count()

    # ── Check 1: Non-empty — early exit if table is empty ─────────────────
    non_empty_result = _run_pydeequ_check(
        spark, df,
        check_name="non_empty",
        error_type="empty_table",
        check=Check(spark, CheckLevel.Error, f"{table_name}:non_empty")
             .hasSize(lambda size: size >= 1),
        total_rows=total_rows,
    )
    g_dq_status.labels(table=table_name, check="non_empty").set(1 if non_empty_result.passed else 0)

    if not non_empty_result.passed:
        severity, sev_label = _dq_severity_scoped("empty_table", 0, 0)
        logger.error(
            "[DQ] ✗ FAIL | table=%s | check=non_empty | sev=%s (%s) | table has 0 rows",
            table_name, sev_label, severity,
        )
        alert.send_alert(
            summary=f"[CDC DQ {sev_label}] '{table_name}': non_empty FAILED — table has 0 rows",
            severity=severity,
            table=table_name,
            error_type="empty_table",
            group="data-quality",
            component="spark/dq_checks",
            details={
                "sev_label":            sev_label,
                "consecutive_failures": consecutive_failures,
                "check":                "non_empty",
                "message":              "table has 0 rows",
                "dashboard_url":        dashboard_url,
            },
        )
        return False

    # ── Check 2: Null PK per column ────────────────────────────────────────
    for pk_col in pk_cols:
        results.append(_run_pydeequ_check(
            spark, df,
            check_name=f"null_pk:{pk_col}",
            error_type="null_pk",
            check=Check(spark, CheckLevel.Error, f"{table_name}:null_pk:{pk_col}")
                 .isComplete(pk_col),
            total_rows=total_rows,
        ))

    # ── Check 3: Unique PK (composite-key aware) ───────────────────────────
    results.append(_run_pydeequ_check(
        spark, df,
        check_name="unique_pk:" + "+".join(pk_cols),
        error_type="duplicate_pk",
        check=Check(spark, CheckLevel.Error, f"{table_name}:unique_pk")
             .hasUniqueness(pk_cols, lambda value: value == 1.0),
        total_rows=total_rows,
    ))

    # ── Check 4: Valid cdc_op ──────────────────────────────────────────────
    if "cdc_op" in df.columns:
        results.append(_run_pydeequ_check(
            spark, df,
            check_name="valid_cdc_op",
            error_type="invalid_cdc_op",
            check=Check(spark, CheckLevel.Error, f"{table_name}:valid_cdc_op")
                 .isContainedIn("cdc_op", ["c", "u", "r"]),
            total_rows=total_rows,
        ))

    # ── Check 5: Freshness (manual — not pydeequ) ──────────────────────────
    if "cdc_ts_ms" in df.columns:
        try:
            now_ms  = int(time.time() * 1000)
            max_ts  = df.agg(F.max("cdc_ts_ms")).collect()[0][0]
            if max_ts is not None:
                latency_seconds = (now_ms - max_ts) / 1000.0
                passed = latency_seconds <= freshness_threshold_seconds
                results.append(DQResult(
                    check_name="freshness",
                    passed=passed,
                    message=(
                        f"latest event is {latency_seconds:.1f} seconds old "
                        f"(threshold: {freshness_threshold_seconds} seconds)"
                    ),
                    error_type="stale_data",
                    violation_rows=0 if passed else 1,  # freshness is a batch-level metric
                    total_rows=total_rows,
                ))
        except Exception as exc:
            logger.warning("[DQ] Could not compute freshness for '%s': %s", table_name, exc)

    # ── Dispatch alerts for all failed checks ─────────────────────────────
    all_passed = True
    for r in results:
        g_dq_status.labels(table=table_name, check=r.check_name).set(1 if r.passed else 0)

        if r.passed:
            logger.info(
                "[DQ] ✓ PASS | table=%s | check=%s | %s",
                table_name, r.check_name, r.message,
            )
        else:
            all_passed = False
            severity, sev_label = _dq_severity_scoped(
                r.error_type, r.violation_rows, r.total_rows
            )
            violation_pct = (
                f"{(r.violation_rows / r.total_rows * 100):.2f}%"
                if r.total_rows > 0 else "N/A"
            )
            logger.error(
                "[DQ] ✗ FAIL | table=%s | check=%s | sev=%s (%s) | "
                "violation_rows=%d/%d (%s) | %s",
                table_name, r.check_name, sev_label, severity,
                r.violation_rows, r.total_rows, violation_pct, r.message,
            )
            alert.send_alert(
                summary=(
                    f"[CDC DQ {sev_label}] '{table_name}': {r.check_name} FAILED — "
                    f"{r.violation_rows}/{r.total_rows} rows ({violation_pct}) | {r.message}"
                ),
                severity=severity,
                table=table_name,
                error_type=r.error_type,
                group="data-quality",
                component="spark/dq_checks",
                details={
                    "sev_label":            sev_label,
                    "consecutive_failures": consecutive_failures,
                    "check":                r.check_name,
                    "message":              r.message,
                    "violation_rows":       r.violation_rows,
                    "total_rows":           r.total_rows,
                    "violation_pct":        violation_pct,
                    "dashboard_url":        dashboard_url,
                },
            )

    if all_passed:
        logger.info("[DQ] ✓ ALL CHECKS PASSED for table '%s'", table_name)
    else:
        logger.error("[DQ] ✗ SOME CHECKS FAILED for table '%s'", table_name)

    return all_passed