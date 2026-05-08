import os
import logging
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger("cdc_processor.alert")

PAGERDUTY_ROUTING_KEY = os.getenv("PAGERDUTY_ROUTING_KEY", "")
PAGERDUTY_API_URL     = "https://events.pagerduty.com/v2/enqueue"
REQUEST_TIMEOUT_SEC   = 10
PAGERDUTY_DEDUP_MODE  = os.getenv("PAGERDUTY_DEDUP_MODE", "stable").strip().lower()


def send_alert(
    summary: str,
    severity: str,
    table: str,
    error_type: str,
    details: Optional[dict] = None,
    dedup_suffix: Optional[str] = None,
    group: str = "data-quality",
    component: Optional[str] = None,
) -> bool:
    """
    Send a PagerDuty trigger event.

    Args:
        summary    : Short description of the incident (displayed in PagerDuty).
        severity   : "critical" | "error" | "warning" | "info"
        table      : Name of the affected table (e.g. "orders").
        error_type : Type of error (e.g. "null_pk", "duplicate_pk", "deequ_check_failed").
        details    : Dict owned by the call site — must include consecutive_failures,
                     proof fields, and dashboard_url. No fields are auto-injected.
        group      : PagerDuty group — "data-quality" or "infrastructure".
        component  : Subsystem that fired — e.g. "spark/dq_checks", "spark/freshness_check".
                     Defaults to "spark-cdc/{table}" if not provided.

    Returns:
        True if sent successfully, False otherwise (includes empty key case).
    """
    timestamp_utc = datetime.now(timezone.utc).isoformat()

    if not PAGERDUTY_ROUTING_KEY:
        logger.warning(
            "[PagerDuty] PAGERDUTY_ROUTING_KEY is not configured. "
            "Alert skipped (graceful degradation). "
            "Incident will be sent when a real routing key is available. | "
            "table=%s | error_type=%s | severity=%s | summary=%s",
            table, error_type, severity, summary,
        )
        return False

    custom_details = details or {}

    dedup_key = _build_dedup_key(table, error_type, dedup_suffix=dedup_suffix)

    payload = {
        "routing_key":  PAGERDUTY_ROUTING_KEY,
        "event_action": "trigger",
        "dedup_key":    dedup_key,
        "payload": {
            "summary":    summary,
            "severity":   severity,         
            "source":     "cdc-processor",
            "timestamp":  timestamp_utc,
            "component":  component or f"spark-cdc/{table}",
            "group":      group,
            "class":      error_type,
            "custom_details": custom_details,
        },
    }

    try:
        response = requests.post(
            PAGERDUTY_API_URL,
            json=payload,
            timeout=REQUEST_TIMEOUT_SEC,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()

        logger.info(
            "[PagerDuty] Incident triggered successfully. "
            "dedup_key=%s | severity=%s | status=%d",
            dedup_key, severity, response.status_code,
        )
        return True

    except requests.exceptions.Timeout:
        logger.error(
            "[PagerDuty] Request timed out after %ds. "
            "table=%s | error_type=%s",
            REQUEST_TIMEOUT_SEC, table, error_type,
        )
        return False

    except requests.exceptions.RequestException as exc:
        logger.error(
            "[PagerDuty] Failed to send alert. table=%s | error_type=%s | error=%s",
            table, error_type, exc,
        )
        return False


def _build_dedup_key(table: str, error_type: str, dedup_suffix: Optional[str] = None) -> str:
    """
    Build PagerDuty dedup_key with configurable mode.

    Modes:
      - stable (default): same issue -> same incident
      - daily: new incident per day (UTC)
      - unique: new incident every trigger
    """
    base_key = f"cdc-{table}-{error_type}"
    if dedup_suffix:
        return f"{base_key}-{dedup_suffix}"

    mode = PAGERDUTY_DEDUP_MODE
    if mode == "daily":
        return f"{base_key}-{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    if mode == "unique":
        return f"{base_key}-{int(datetime.now(timezone.utc).timestamp())}"
    return base_key


def resolve_alert(table: str, error_type: str, dedup_suffix: Optional[str] = None) -> bool:
    """
    Send a PagerDuty resolve event to automatically close an incident when the issue is fixed.

    Args:
        table      : Table name (must match the dedup_key used at trigger time).
        error_type : Error type (must match the dedup_key used at trigger time).

    Returns:
        True if sent successfully, False otherwise.
    """
    if not PAGERDUTY_ROUTING_KEY:
        logger.debug(
            "[PagerDuty] Resolve skipped — routing key is not configured. "
            "table=%s | error_type=%s", table, error_type,
        )
        return False

    payload = {
        "routing_key":  PAGERDUTY_ROUTING_KEY,
        "event_action": "resolve",
        "dedup_key":    _build_dedup_key(table, error_type, dedup_suffix=dedup_suffix),
    }

    try:
        response = requests.post(
            PAGERDUTY_API_URL,
            json=payload,
            timeout=REQUEST_TIMEOUT_SEC,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        logger.info(
            "[PagerDuty] Incident resolved. dedup_key=cdc-%s-%s",
            table, error_type,
        )
        return True

    except requests.exceptions.RequestException as exc:
        logger.error(
            "[PagerDuty] Failed to resolve alert. table=%s | error_type=%s | error=%s",
            table, error_type, exc,
        )
        return False