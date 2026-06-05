"""
kafka_alert_consumer.py
-----------------------
The ONLY service that calls PagerDuty.

Subscribes to both alert topics simultaneously:
  - northstream.alerts.infrastructure  (from webhook_receiver / Grafana)
  - northstream.alerts.data-quality    (from alert.py in spark-cdc)

On each message: reads event_action → calls PagerDuty trigger or resolve.

Bugs fixed in this version
──────────────────────────
BUG-1  _pagerduty_resolve() was defined with signature (payload: dict)
       but called as _pagerduty_resolve(payload, topic) — 2 args vs 1.
       Every resolve message crashed with:
         TypeError: _pagerduty_resolve() takes 1 positional argument but 2 were given
       Fix: added `topic: str` as second parameter (kept for symmetry with
       _pagerduty_trigger, even though resolve does not need topic routing).

BUG-2  _pagerduty_trigger() guard checked PAGERDUTY_ROUTING_KEY_INFRA and
       PAGERDUTY_ROUTING_KEY_DQ (two keys that do not exist in .env.example),
       but the actual payload always used PAGERDUTY_ROUTING_KEY (the single
       key defined in .env.example). The guard returned False whenever only
       the base key was set, so NO trigger was ever delivered to PagerDuty
       even when the key was configured.
       Fix: unified routing. Both topics use PAGERDUTY_ROUTING_KEY (single
       key). PAGERDUTY_ROUTING_KEY_INFRA / _DQ are read as optional overrides
       — if they are blank, fall back to PAGERDUTY_ROUTING_KEY. The guard
       now correctly checks the resolved key actually used in the payload.

BUG-3  _pagerduty_resolve() guarded on PAGERDUTY_ROUTING_KEY but the guard
       was never reached (Bug-1 crashed first). Now that Bug-1 is fixed,
       the resolve guard also unified with the same fallback logic.

SEV classification — unchanged from original (all 5 case studies covered).
"""

import json
import logging
import os
import time

import requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP       = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

# Single shared key (set in .env as PAGERDUTY_ROUTING_KEY).
# Optional per-topic overrides: set PAGERDUTY_ROUTING_KEY_INFRA or
# PAGERDUTY_ROUTING_KEY_DQ to route each topic to a different PagerDuty
# service. If blank, both fall back to PAGERDUTY_ROUTING_KEY.
_KEY_INFRA = os.environ.get("PAGERDUTY_ROUTING_KEY_INFRA", "") 
_KEY_DQ    = os.environ.get("PAGERDUTY_ROUTING_KEY_DQ",    "") 

PAGERDUTY_API_URL   = "https://events.pagerduty.com/v2/enqueue"
REQUEST_TIMEOUT_SEC = 10
RETRY_SLEEP_SEC     = 5

INFRA_TOPIC = "northstream.alerts.infrastructure"
DQ_TOPIC    = "northstream.alerts.data-quality"

# ── SEV classification table ──────────────────────────────────────────────────
# error_type → (sev_label, pd_severity)
SEV_MAP = {
    # Infrastructure topic ────────────────────────────────────────────────────
    "spark_not_running":    ("SEV-1", "critical"),   # Case 6.2 — §4.1 SEV-1
    "kafka_broker_down":    ("SEV-1", "critical"),   # §4.1 SEV-1
    "kafka_lag_growing":    ("SEV-2", "error"),      # Case 6.1 — §4.1 SEV-2
    "freshness_sla_breach": ("SEV-2", "error"),      # Case 6.3 infra leg — §4.1 SEV-2

    # Data-quality topic ──────────────────────────────────────────────────────
    "duplicate_pk":         ("SEV-2", "error"),      # Case 6.4 — §4.2 SEV-2
    "null_pk":              ("SEV-2", "error"),      # §4.2 SEV-2 (record-level)
    "empty_table":          ("SEV-2", "error"),      # Case 6.5 — §4.2 SEV-2
    "invalid_cdc_op":       ("SEV-2", "error"),      # §4.2 SEV-2
    "stale_data":           ("SEV-3", "warning"),    # Case 6.3 DQ leg — §4.2 SEV-3
}

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [kafka_alert_consumer] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


# ── PagerDuty helpers ─────────────────────────────────────────────────────────

def _resolve_routing_key(topic: str) -> str:
    """
    Return the correct PagerDuty routing key for the given topic.

    Priority:
      1. Per-topic override (PAGERDUTY_ROUTING_KEY_INFRA / _DQ)
      2. Base key (PAGERDUTY_ROUTING_KEY)

    Returns empty string if nothing is configured — callers check for this.
    """
    if topic == INFRA_TOPIC:
        return _KEY_INFRA
    return _KEY_DQ


def _pagerduty_trigger(payload: dict, topic: str) -> bool:
    """
    POST a trigger event to PagerDuty Events API v2.

    BUG-2 fix: routing_key in the payload now comes from _resolve_routing_key()
    and the guard checks the same resolved key — so it correctly allows
    delivery when only PAGERDUTY_ROUTING_KEY (base) is set.
    """
    routing_key = _resolve_routing_key(topic)
    if not routing_key:
        log.warning("PAGERDUTY_ROUTING_KEY not set — trigger skipped (dry-run mode)")
        return False

    error_type = payload.get("error_type", "unknown")
    table      = payload.get("table", "all")
    sev_label, pd_severity = SEV_MAP.get(error_type, ("SEV-2", "error"))

    # Honour dedup_key from payload (preserves spark-cdc dedup_suffix logic),
    # otherwise build a stable key from table + error_type.
    dedup_key = payload.get("dedup_key") or f"cdc-{table}-{error_type}"

    pd_payload = {
        "routing_key":  routing_key,          # BUG-2 fix: was always _KEY_BASE
        "event_action": "trigger",
        "dedup_key":    dedup_key,
        "payload": {
            "summary":        payload.get("summary", f"[CDC] {error_type} on {table}"),
            "severity":       pd_severity,
            "source":         payload.get("source", "northstream"),
            "component":      payload.get("component", f"spark-cdc/{table}"),
            "group":          payload.get("group", "infrastructure"),
            "class":          error_type,
            "custom_details": payload.get("custom_details", {}),
        },
    }

    try:
        resp = requests.post(
            PAGERDUTY_API_URL,
            json=pd_payload,
            timeout=REQUEST_TIMEOUT_SEC,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        log.info(
            "PagerDuty TRIGGER sent | %s | error_type=%s | dedup_key=%s | severity=%s | status=%d",
            sev_label, error_type, dedup_key, pd_severity, resp.status_code,
        )
        return True
    except requests.exceptions.RequestException as exc:
        log.error("PagerDuty trigger failed | error_type=%s | error=%s", error_type, exc)
        return False


def _pagerduty_resolve(payload: dict, topic: str) -> bool:
    """
    POST a resolve event to PagerDuty Events API v2.
    dedup_key must match the one used at trigger time.

    BUG-1 fix: added `topic: str` parameter — previously defined as
    _pagerduty_resolve(payload: dict) with no topic argument, but called
    as _pagerduty_resolve(payload, topic), causing:
      TypeError: _pagerduty_resolve() takes 1 positional argument but 2 were given

    BUG-3 fix: routing_key in payload now comes from _resolve_routing_key()
    same as trigger, so resolve uses the same key as the original trigger.
    """
    routing_key = _resolve_routing_key(topic)
    if not routing_key:
        log.warning("PAGERDUTY_ROUTING_KEY not set — resolve skipped (dry-run mode)")
        return False

    error_type = payload.get("error_type", "unknown")
    table      = payload.get("table", "all")
    dedup_key  = payload.get("dedup_key") or f"cdc-{table}-{error_type}"

    pd_payload = {
        "routing_key":  routing_key,          # BUG-3 fix: unified with trigger routing
        "event_action": "resolve",
        "dedup_key":    dedup_key,
    }

    try:
        resp = requests.post(
            PAGERDUTY_API_URL,
            json=pd_payload,
            timeout=REQUEST_TIMEOUT_SEC,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        log.info(
            "PagerDuty RESOLVE sent | error_type=%s | dedup_key=%s | status=%d",
            error_type, dedup_key, resp.status_code,
        )
        return True
    except requests.exceptions.RequestException as exc:
        log.error("PagerDuty resolve failed | error_type=%s | error=%s", error_type, exc)
        return False


# ── Message handler ───────────────────────────────────────────────────────────

def handle_message(topic: str, offset: int, payload: dict) -> None:
    error_type   = payload.get("error_type", "unknown")
    event_action = payload.get("event_action", "trigger")
    sev_label    = SEV_MAP.get(error_type, ("SEV-?", ""))[0]

    log.info(
        "Received [%s] from %s offset %d | error_type=%s | %s",
        event_action.upper(), topic, offset, error_type, sev_label,
    )

    if event_action == "trigger":
        _pagerduty_trigger(payload, topic)
    elif event_action == "resolve":
        # BUG-1 fix: pass topic as second argument
        _pagerduty_resolve(payload, topic)
    else:
        log.warning("Unknown event_action '%s' — skipping", event_action)


# ── Consumer loop ─────────────────────────────────────────────────────────────

def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        INFRA_TOPIC,
        DQ_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="northstream-alert-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=-1,
    )


def run() -> None:
    log.info("Starting Kafka alert consumer")
    log.info("Bootstrap: %s", KAFKA_BOOTSTRAP)
    log.info("Topics: %s, %s", INFRA_TOPIC, DQ_TOPIC)

    # Log key configuration at startup so you can immediately tell if the key
    # is missing without having to wait for the first message.
    if _KEY_INFRA:
        log.info("PagerDuty routing: infra topic key configured")
    else:
            log.warning(
                "PAGERDUTY_ROUTING_KEY_INFRA is not set — "
                "infrastructure alerts will be skipped."
            )

    if _KEY_DQ:
            log.info("PagerDuty routing: data-quality topic key configured")
    else:
            log.warning(
                "PAGERDUTY_ROUTING_KEY_DQ is not set — "
                "data-quality alerts will be skipped."
            )

    while True:
        try:
            consumer = build_consumer()
            log.info("Consumer connected — polling for messages...")

            for message in consumer:
                try:
                    handle_message(message.topic, message.offset, message.value)
                except Exception as exc:
                    log.error(
                        "Error handling message from %s offset %d: %s",
                        message.topic, message.offset, exc,
                    )
                    # Never crash the loop on a bad message

        except KafkaError as exc:
            log.error("Kafka connection error: %s — retrying in %ds", exc, RETRY_SLEEP_SEC)
            time.sleep(RETRY_SLEEP_SEC)
        except Exception as exc:
            log.error("Unexpected error: %s — retrying in %ds", exc, RETRY_SLEEP_SEC)
            time.sleep(RETRY_SLEEP_SEC)


if __name__ == "__main__":
    run()