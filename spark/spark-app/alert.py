"""
alert.py
--------
Produces alert events to Kafka instead of calling PagerDuty directly.

Public API (signatures UNCHANGED — deequ_checks.py and cdc_processor.py
call sites require zero modifications):

    send_alert(summary, severity, table, error_type,
               details, dedup_suffix, group, component) -> bool

    resolve_alert(table, error_type, dedup_suffix) -> bool

All PagerDuty logic has moved to alerting/kafka_alert_consumer.py.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger("cdc_processor.alert")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_DQ_TOPIC   = os.getenv("KAFKA_DQ_TOPIC", "northstream.alerts.data-quality")

# Kept so test_dq_alert.py dry-run detection (`alert.PAGERDUTY_ROUTING_KEY`)
# continues to work without modification.
PAGERDUTY_ROUTING_KEY = os.getenv("PAGERDUTY_ROUTING_KEY", "")

# ── Kafka producer (lazy singleton) ───────────────────────────────────────────
_producer: Optional[KafkaProducer] = None


def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            retries=3,
        )
        logger.info("[Alert] KafkaProducer connected to %s", KAFKA_BOOTSTRAP)
    return _producer


# ── Public API ────────────────────────────────────────────────────────────────

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
    Produce a trigger event to northstream.alerts.data-quality.

    Signature is identical to the previous PagerDuty implementation.
    All args are forwarded as-is into the Kafka message payload so the
    consumer can reconstruct a complete PagerDuty event.

    Returns True if the message was produced and flushed, False otherwise.
    """
    timestamp_utc = datetime.now(timezone.utc).isoformat()
    dedup_key     = _build_dedup_key(table, error_type, dedup_suffix=dedup_suffix)
    msg_key       = f"{table}:{error_type}"

    payload = {
        "event_action":   "trigger",
        "dedup_key":      dedup_key,
        "summary":        summary,
        "severity":       severity,
        "table":          table,
        "error_type":     error_type,
        "group":          group,
        "component":      component or f"spark-cdc/{table}",
        "source":         "spark-cdc",
        "timestamp":      timestamp_utc,
        "custom_details": details or {},
    }

    try:
        producer = _get_producer()
        producer.send(KAFKA_DQ_TOPIC, key=msg_key, value=payload)
        producer.flush()
        logger.info(
            "[Alert] Produced TRIGGER → %s | key=%s | severity=%s | dedup_key=%s",
            KAFKA_DQ_TOPIC, msg_key, severity, dedup_key,
        )
        return True

    except KafkaError as exc:
        logger.error(
            "[Alert] Failed to produce trigger. table=%s | error_type=%s | error=%s",
            table, error_type, exc,
        )
        return False
    except Exception as exc:  
        logger.error(
            "[Alert] Unexpected error producing trigger. table=%s | error=%s",
            table, exc,
        )
        return False


def resolve_alert(
    table: str,
    error_type: str,
    dedup_suffix: Optional[str] = None,
) -> bool:
    """
    Produce a resolve event to northstream.alerts.data-quality.

    Signature is identical to the previous PagerDuty implementation.
    The consumer will call PagerDuty resolve using the matching dedup_key.

    Returns True if the message was produced and flushed, False otherwise.
    """
    dedup_key = _build_dedup_key(table, error_type, dedup_suffix=dedup_suffix)
    msg_key   = f"{table}:{error_type}"

    payload = {
        "event_action": "resolve",
        "dedup_key":    dedup_key,
        "table":        table,
        "error_type":   error_type,
        "source":       "spark-cdc",
        "timestamp":    datetime.now(timezone.utc).isoformat(),
    }

    try:
        producer = _get_producer()
        producer.send(KAFKA_DQ_TOPIC, key=msg_key, value=payload)
        producer.flush()
        logger.info(
            "[Alert] Produced RESOLVE → %s | key=%s | dedup_key=%s",
            KAFKA_DQ_TOPIC, msg_key, dedup_key,
        )
        return True

    except KafkaError as exc:
        logger.error(
            "[Alert] Failed to produce resolve. table=%s | error_type=%s | error=%s",
            table, error_type, exc,
        )
        return False
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "[Alert] Unexpected error producing resolve. table=%s | error=%s",
            table, exc,
        )
        return False


# ── Internal helpers ──────────────────────────────────────────────────────────

def _build_dedup_key(
    table: str,
    error_type: str,
    dedup_suffix: Optional[str] = None,
) -> str:
    """
    Builds the dedup_key forwarded to PagerDuty by the consumer.
    Logic is identical to the previous implementation so existing
    test_dq_alert.py T2 trigger/resolve pairs continue to match.
    """
    base_key = f"cdc-{table}-{error_type}"
    if dedup_suffix:
        return f"{base_key}-{dedup_suffix}"
    return base_key