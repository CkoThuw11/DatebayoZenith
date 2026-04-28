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

Implemented in **native PySpark** (compatible with Spark 3.5.x, không dùng pydeequ runtime).

Bốn checks chạy **per table** sau mỗi chu kỳ Avro→Parquet:

| # | Check | Metric label | Điều kiện |
|---|---|---|---|
| 1 | **Non-empty** | `non_empty` | Table phải có ≥ 1 row |
| 2 | **Null PK** | `null_pk:<col>` | Không có PK column nào là NULL |
| 3 | **Uniqueness PK** | `unique_pk:<col>` | Không có PK trùng lặp |
| 4 | **Valid `cdc_op`** | `valid_cdc_op` | `cdc_op` ∈ `{c, u, r}` — deletes (`d`) bị loại |

Mỗi kết quả check được emit ra Prometheus:

```
cdc_dq_check_status{table="orders", check="null_pk:order_id"} 1.0   # 1=pass, 0=fail
```

### Composite PK support

Table có composite PK (`order_details`: `order_id + product_id`) sẽ tự động chạy null + uniqueness check cho từng cột.

---

## PagerDuty Alerting (`alert.py`)

### `send_alert(summary, severity, table, error_type, details)`

Fires a **trigger** event tới PagerDuty Events API v2.

| Field | Value |
|---|---|
| Endpoint | `https://events.pagerduty.com/v2/enqueue` |
| `dedup_key` | `cdc-{table}-{error_type}` — tránh tạo duplicate incidents cho cùng table + lỗi |
| `severity` | `critical` cho DQ failures và unhandled exceptions |
| `custom_details` | `table`, `error_type`, `timestamp`, `source`, context cụ thể của check |
| Timeout | 10 s |

### `resolve_alert(table, error_type)`

Gửi **resolve** event với cùng `dedup_key` để tự động đóng incident sau khi vấn đề được khắc phục.

### Graceful degradation

Nếu `PAGERDUTY_ROUTING_KEY` chưa được set, `send_alert` chỉ log `WARNING` và return `False` — **pipeline vẫn tiếp tục chạy**, không crash.

---

## Grafana Alert Rules (`alerting.yaml`)

Được provisioned tự động khi Grafana khởi động.

### Contact Point

**PagerDuty CDC Alerts** → `https://events.eu.pagerduty.com/v2/enqueue`  
Integration key lấy từ `${PAGERDUTY_ROUTING_KEY}` env var.

### Alert Rules (group: `CDC Data Quality & Kafka Lag`, evaluate mỗi 1 phút)

| Rule | Điều kiện | Fire sau | Severity |
|---|---|---|---|
| **Kafka Consumer Lag Too High** | `cdc_kafka_consumer_lag > 1000` messages | 1 min liên tục | critical |
| **Spark Processor Not Running** | `time() - cdc_last_run_timestamp_seconds > 600` (10 phút) | 2 min liên tục | critical |

Notification policy: group by `alertname + table`, wait 30s, repeat mỗi 4h.

---

## Configuration

### Thêm PagerDuty Integration Key vào `.env`

```env
# PagerDuty Events API v2 — Integration Key
# Lấy tại: PagerDuty → Services → <service> → Integrations → Add integration → Events API v2
PAGERDUTY_ROUTING_KEY=your_32char_key_here
```

> Để trống → chạy ở chế độ **graceful degradation** (DQ checks vẫn chạy, alerts chỉ ghi log).

---

## Running the Test Suite

Test end-to-end (`test_dq_alert.py`) gồm 5 scenarios, chạy trong container:

```powershell
docker exec spark-cdc /opt/spark/bin/spark-submit `
  --master local[1] `
  --jars "/opt/spark/extra-jars/hadoop-aws.jar,/opt/spark/extra-jars/aws-java-sdk-bundle.jar" `
  /app/test_dq_alert.py
```

| Scenario | Input | Kết quả mong đợi |
|---|---|---|
| 1 — Good data | 3 valid rows | ALL PASS, không alert |
| 2 — NULL PK | `order_id = NULL` ở 1 row | `null_pk` FAIL + PagerDuty alert |
| 3 — Duplicate PK | 2 rows cùng `order_id` | `unique_pk` FAIL + PagerDuty alert |
| 4 — Invalid `cdc_op` | `cdc_op = 'd'` | `valid_cdc_op` FAIL + PagerDuty alert |
| 5 — Empty table | 0 rows | `non_empty` FAIL + PagerDuty alert |

---

## Verified Outcomes

- ✅ DQ checks chạy sau mỗi chu kỳ Avro→Parquet cho cả 4 tables
- ✅ Row lỗi (null PK / duplicate) trigger check failure
- ✅ PagerDuty incident fire trong vòng 2 phút kể từ failure
- ✅ Alert payload gồm: `table`, `error_type`, `timestamp`, `check`, `message`
- ✅ `resolve_alert` tự đóng incident qua `dedup_key`
- ✅ Pipeline không crash khi `PAGERDUTY_ROUTING_KEY` chưa được set
- ✅ Prometheus metric `cdc_dq_check_status` hiển thị trên Grafana
- ✅ Grafana rules cho Kafka lag (> 1000 msgs) và Spark stale (> 10 min) được provision sẵn
