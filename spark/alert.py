"""
alert.py — PagerDuty Alerting Module
--------------------------------------
Gọi PagerDuty Events API v2 để tạo incident khi phát hiện lỗi
trong quá trình xử lý CDC pipeline.

Graceful degradation: Nếu PAGERDUTY_ROUTING_KEY trống (chưa cấu hình),
module chỉ ghi log WARNING thay vì crash — pipeline vẫn tiếp tục chạy.

Tham khảo: https://developer.pagerduty.com/docs/events-api-v2/trigger-events/
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger("cdc_processor.alert")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PAGERDUTY_ROUTING_KEY = os.getenv("PAGERDUTY_ROUTING_KEY", "")
PAGERDUTY_API_URL     = "https://events.pagerduty.com/v2/enqueue"
REQUEST_TIMEOUT_SEC   = 10


def send_alert(
    summary: str,
    severity: str,
    table: str,
    error_type: str,
    details: Optional[dict] = None,
) -> bool:
    """
    Gửi PagerDuty trigger event.

    Args:
        summary    : Mô tả ngắn gọn về sự cố (hiển thị trong PagerDuty).
        severity   : "critical" | "error" | "warning" | "info"
        table      : Tên bảng bị lỗi (vd: "orders").
        error_type : Loại lỗi (vd: "null_pk", "duplicate_pk", "deequ_check_failed").
        details    : Dict tuỳ chọn chứa thông tin bổ sung (rows, check_name, ...).

    Returns:
        True nếu gửi thành công, False nếu không (bao gồm cả key trống).
    """
    timestamp_utc = datetime.now(timezone.utc).isoformat()

    # ── Graceful degradation: không có key → log và bỏ qua ──────────────────
    if not PAGERDUTY_ROUTING_KEY:
        logger.warning(
            "[PagerDuty] PAGERDUTY_ROUTING_KEY chưa được cấu hình. "
            "Alert bị bỏ qua (graceful degradation). "
            "Incident sẽ được gửi khi có routing key thực. | "
            "table=%s | error_type=%s | severity=%s | summary=%s",
            table, error_type, severity, summary,
        )
        return False

    # ── Xây dựng payload theo PagerDuty Events API v2 ───────────────────────
    custom_details = {
        "table":      table,
        "error_type": error_type,
        "timestamp":  timestamp_utc,
        "source":     "spark-cdc-processor",
    }
    if details:
        custom_details.update(details)

    payload = {
        "routing_key":  PAGERDUTY_ROUTING_KEY,
        "event_action": "trigger",
        # dedup_key: tránh tạo duplicate incidents cho cùng table + error_type
        "dedup_key":    f"cdc-{table}-{error_type}",
        "payload": {
            "summary":    summary,
            "severity":   severity,          # critical | error | warning | info
            "source":     "cdc-processor",
            "timestamp":  timestamp_utc,
            "component":  f"spark-cdc/{table}",
            "group":      "data-quality",
            "class":      error_type,
            "custom_details": custom_details,
        },
    }

    # ── Gửi HTTP request ─────────────────────────────────────────────────────
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
            "dedup_key=cdc-%s-%s | severity=%s | status=%d",
            table, error_type, severity, response.status_code,
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


def resolve_alert(table: str, error_type: str) -> bool:
    """
    Gửi PagerDuty resolve event để đóng incident tự động khi vấn đề được khắc phục.

    Args:
        table      : Tên bảng (phải khớp với dedup_key lúc trigger).
        error_type : Loại lỗi (phải khớp với dedup_key lúc trigger).

    Returns:
        True nếu gửi thành công, False nếu không.
    """
    if not PAGERDUTY_ROUTING_KEY:
        logger.debug(
            "[PagerDuty] Resolve skipped — routing key không được cấu hình. "
            "table=%s | error_type=%s", table, error_type,
        )
        return False

    payload = {
        "routing_key":  PAGERDUTY_ROUTING_KEY,
        "event_action": "resolve",
        "dedup_key":    f"cdc-{table}-{error_type}",
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
