import json
import logging
import os
from typing import Optional
from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
INFRA_TOPIC     = "northstream.alerts.infrastructure"

# Grafana alert title → error_type string expected by the consumer
ALERT_TITLE_MAP = {
    "CDC Kafka Consumer Lag Too High":  "kafka_lag_growing",
    "CDC Spark Processor Not Running":  "spark_not_running",
    "CDC Data Freshness SLA Breach":    "freshness_sla_breach",
    "CDC Kafka Broker Unreachable":     "kafka_broker_down",
}

# Grafana internal / synthetic titles — skip silently, no WARNING spam
SILENT_SKIP = {
    "DatasourceNoData",
    "DatasourceError",
    "DatasourceNoData resolved",
}

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [webhook_receiver] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Kafka producer (lazy singleton) ───────────────────────────────────────────
_producer: Optional[KafkaProducer] = None

def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            retries=3,
        )
        log.info("KafkaProducer connected to %s", KAFKA_BOOTSTRAP)
    return _producer

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(__name__)


@app.route("/alert", methods=["POST"])
def receive_alert():
    """
    Accepts Grafana unified alerting webhook payload.
    Produces one message per alert to northstream.alerts.infrastructure.
    Always returns HTTP 200 so Grafana does not retry.
    """
    body = request.get_json(silent=True) or {}
    alerts = body.get("alerts", [])

    if not alerts:
        log.debug("Webhook body had no alerts array (keys=%s)", list(body.keys()))
        return jsonify({"status": "ok", "produced": 0}), 200

    produced = 0
    for alert in alerts:
        title         = alert.get("labels", {}).get("alertname", "")
        grafana_state = alert.get("status", "firing")
        labels        = alert.get("labels", {})
        annotations   = alert.get("annotations", {})

        # Silently skip Grafana internal system titles
        if title in SILENT_SKIP:
            log.debug("Silently skipping internal Grafana title '%s'", title)
            continue

        # Silently skip emoji SEV-level synthetic rules from 1-minute-checks group
        if any(title.startswith(p) for p in ("\U0001f534", "\U0001f7e0", "\U0001f7e1", "\U0001f535")):
            log.debug("Silently skipping SEV synthetic alert '%s'", title)
            continue

        error_type = ALERT_TITLE_MAP.get(title)
        if not error_type:
            log.warning("Unknown alert title '%s' — add to ALERT_TITLE_MAP if needed", title)
            continue

        event_action = "trigger" if grafana_state == "firing" else "resolve"

        message = {
            "event_action": event_action,
            "error_type":   error_type,
            "alert_title":  title,
            "severity":     labels.get("severity", "error"),
            "group":        labels.get("group", "infrastructure"),
            "component":    labels.get("component", ""),
            "summary":      annotations.get("summary", title),
            "table":        labels.get("table", "all"),
            "source":       "grafana",
        }

        msg_key = f"infra:{error_type}"

        try:
            producer = get_producer()
            producer.send(INFRA_TOPIC, key=msg_key, value=message)
            producer.flush()
            log.info(
                "Produced [%s] %s → %s (key=%s)",
                event_action.upper(), error_type, INFRA_TOPIC, msg_key,
            )
            produced += 1
        except KafkaError as exc:
            log.error("Failed to produce to Kafka: %s", exc)

    return jsonify({"status": "ok", "produced": produced}), 200


# Both /health and /healthz respond — covers any docker-compose healthcheck variant
@app.route("/health", methods=["GET"])
@app.route("/healthz", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)