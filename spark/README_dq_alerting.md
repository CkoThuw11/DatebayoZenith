# feat/data-quality-alerting — Data Quality & PagerDuty Alerting

> **Branch goal:** Integrate automated data quality checks and real-time incident alerting into the Northwind CDC pipeline.

---

## Overview

This branch extends the Spark CDC processor with two complementary capabilities:

1. **Data Quality (DQ) checks** — native PySpark checks run automatically after every Avro→Parquet write cycle, covering all 4 tables (`orders`, `order_details`, `products`, `customers`).
2. **PagerDuty alerting** — any DQ failure or unhandled pipeline exception fires a PagerDuty incident via Events API v2 within ~2 minutes.

```
Avro files (MinIO)
      ↓
  cdc_processor.py  ──write──▶  Parquet (MinIO)
      ↓  (after every table)
  run_dq_checks()
      ├── PASS → Prometheus metric cdc_dq_check_status{check}=1
      └── FAIL → Prometheus metric = 0
                      ↓
                  alert.py  ──POST──▶  PagerDuty Events API v2
                                           (incident fires ≤ 2 min)
```

---

## Files Changed

| File | Change |
|---|---|
| `spark/requirements.txt` | Added `pydeequ==1.3.0`, `requests==2.31.0` |
| `spark/cdc_processor.py` | Added `DQResult` dataclass + `run_dq_checks()`, wired into `process_table()` and unhandled exception handlers |
| `spark/alert.py` | **New** — PagerDuty Events API v2 client (`send_alert`, `resolve_alert`) |
| `spark/test_dq_alert.py` | **New** — End-to-end test suite (5 scenarios) runnable inside the container |
| `spark/Dockerfile` | Pinned base image to `apache/spark:3.5.1`; pre-fetches Deequ JAR at build time |
| `.env.example` | Added `PAGERDUTY_ROUTING_KEY=` placeholder |
| `monitoring/grafana/alerting/alerting.yaml` | **New** — Provisions PagerDuty contact point + 2 alert rules (Kafka lag, Spark stale) |

---

## Data Quality Checks (`run_dq_checks`)

Implemented with **pydeequ** on top of Spark 3.5.x.

Four checks run **per table** after every Avro→Parquet cycle:

| # | Check | Metric label | Condition |
|---|---|---|---|
| 1 | **Non-empty** | `non_empty` | Table must have ≥ 1 row |
| 2 | **Null PK** | `null_pk:<col>` | No primary-key column may be NULL |
| 3 | **Uniqueness PK** | `unique_pk:<col>` | No duplicate primary-key values |
| 4 | **Valid `cdc_op`** | `valid_cdc_op` | `cdc_op` ∈ `{c, u, r}` — deletes (`d`) are excluded |

Each check result is emitted as a Prometheus gauge:

```
cdc_dq_check_status{table="orders", check="null_pk:order_id"} 1.0   # 1=pass, 0=fail
```

### Composite PK support

Tables with composite primary keys (`order_details`: `order_id + product_id`) automatically run null and uniqueness checks for each individual column.

---

## PagerDuty Alerting (`alert.py`)

### `send_alert(summary, severity, table, error_type, details)`

Fires a **trigger** event to the PagerDuty Events API v2.

| Field | Value |
|---|---|
| Endpoint | `https://events.pagerduty.com/v2/enqueue` |
| `dedup_key` | `cdc-{table}-{error_type}` — prevents duplicate incidents for the same table + error |
| `severity` | `critical` for DQ failures and unhandled exceptions |
| `custom_details` | `table`, `error_type`, `timestamp`, `source`, check-specific context |
| Timeout | 10 s |

### `resolve_alert(table, error_type)`

Sends a **resolve** event using the same `dedup_key` to automatically close the incident once the issue is fixed.

### Graceful degradation

If `PAGERDUTY_ROUTING_KEY` is not set, `send_alert` logs a `WARNING` and returns `False` — **the pipeline keeps running without crashing**.

---

## Grafana Alert Rules (`alerting.yaml`)

Provisioned automatically on Grafana startup via the provisioning directory.

### Contact Point

**PagerDuty CDC Alerts** → `https://events.eu.pagerduty.com/v2/enqueue`  
Integration key injected from the `${PAGERDUTY_ROUTING_KEY}` environment variable.

### Alert Rules (group: `CDC Data Quality & Kafka Lag`, evaluated every 1 minute)

| Rule | Condition | Fire after | Severity |
|---|---|---|---|
| **Kafka Consumer Lag Too High** | `cdc_kafka_consumer_lag > 1000` messages | 1 min sustained | critical |
| **Spark Processor Not Running** | `time() - cdc_last_run_timestamp_seconds > 600` (10 min) | 2 min sustained | critical |

Notification policy: group by `alertname + table`, 30 s wait, repeat every 4 h.

---

## Configuration

### Add your PagerDuty Integration Key to `.env`

```env
# PagerDuty Events API v2 — Integration Key
# Obtain from: PagerDuty → Services → <service> → Integrations → Add integration → Events API v2
PAGERDUTY_ROUTING_KEY=your_32char_key_here
```

> Leave blank to run in **graceful degradation** mode (DQ checks still run; alerts are logged only).

---

## Running the Test Suite

The end-to-end test (`test_dq_alert.py`) covers 5 scenarios and runs inside the container:

```powershell
docker exec spark-cdc /opt/spark/bin/spark-submit `
  --master local[1] `
  --jars "/opt/spark/extra-jars/hadoop-aws.jar,/opt/spark/extra-jars/aws-java-sdk-bundle.jar" `
  /app/test_dq_alert.py
```

| Scenario | Input | Expected result |
|---|---|---|
| 1 — Good data | 3 valid rows | ALL PASS, no alert |
| 2 — NULL PK | `order_id = NULL` in one row | `null_pk` FAIL + PagerDuty alert |
| 3 — Duplicate PK | Two rows with same `order_id` | `unique_pk` FAIL + PagerDuty alert |
| 4 — Invalid `cdc_op` | `cdc_op = 'd'` | `valid_cdc_op` FAIL + PagerDuty alert |
| 5 — Empty table | 0 rows | `non_empty` FAIL + PagerDuty alert |

---
## Test
docker exec spark-cdc /opt/spark/bin/spark-submit --master local[1] --jars "/opt/spark/extra-jars/hadoop-aws.jar,/opt/spark/extra-jars/aws-java-sdk-bundle.jar,/opt/spark/extra-jars/deequ.jar" /app/test_dq_alert.py

## Verified Outcomes

- ✅ DQ checks run after every Avro→Parquet cycle for all 4 tables
- ✅ Injected bad row (null PK / duplicate) triggers a check failure
- ✅ PagerDuty incident fires within 2 minutes of failure
- ✅ Alert payload includes: `table`, `error_type`, `timestamp`, `check`, `message`
- ✅ `resolve_alert` auto-closes the incident via `dedup_key`
- ✅ Pipeline runs without crashing when `PAGERDUTY_ROUTING_KEY` is not set
- ✅ Prometheus metric `cdc_dq_check_status` visible in Grafana
- ✅ Grafana rules for Kafka lag (> 1000 msgs) and Spark stale (> 10 min) provisioned automatically
