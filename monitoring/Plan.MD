# Pipeline Monitoring & Grafana Dashboard

Add Prometheus + Pushgateway + Grafana to the DatebayoZenith stack so every CDC run emits metrics and a live dashboard is available at `localhost:3000`.

---

## Architecture Overview

```
cdc_processor.py  ──push──►  Pushgateway :9091
                                   │
Kafka (KRaft)  ─────────────────►  Prometheus :9090  (scrapes Pushgateway + JMX exporter)
                                   │
                              Grafana :3000  (reads Prometheus)
```

**Why Pushgateway?** `cdc_processor.py` is a short-lived batch job called from `entrypoint.sh`. Prometheus can't scrape a process that exits before it polls. Pushgateway keeps the latest metric values alive between runs.

---

## Proposed Changes

### 1. `docker-compose.yaml` — Add three new services

#### [MODIFY] [docker-compose.yaml](file:///d:/DE_Project/DatebayoZenith/docker-compose.yaml)

New services appended **before** the `volumes:` block:

| Service | Image | Port | Role |
|---------|-------|------|------|
| `prometheus` | `prom/prometheus:v2.51.0` | `9090` | Scrape & store metrics |
| `pushgateway` | `prom/pushgateway:v1.8.0` | `9091` | Receive push metrics from Spark batch |
| `grafana` | `grafana/grafana:10.4.2` | `3000` | Dashboard UI (admin/admin) |

New named volumes: `prometheus_data`, `grafana_data`

Prometheus will be given a `./monitoring/prometheus.yml` config file (mounted read-only).  
Grafana will be given a pre-provisioned datasource (`./monitoring/grafana/datasources/prometheus.yaml`) and dashboard JSON (`./monitoring/grafana/dashboards/`).

---

### 2. `monitoring/` — New directory with config files

#### [NEW] `monitoring/prometheus.yml`
```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: pushgateway
    static_configs:
      - targets: ['pushgateway:9091']
    honor_labels: true   # keep "job" label pushed by cdc_processor
```

#### [NEW] `monitoring/grafana/datasources/prometheus.yaml`
Auto-provision Prometheus as Grafana datasource so it works on first boot.

#### [NEW] `monitoring/grafana/dashboards/dashboard.yaml`
Grafana provisioning config to load the dashboard JSON automatically.

#### [NEW] `monitoring/grafana/dashboards/cdc_pipeline.json`
Dashboard with 4 panels:
1. **Kafka Consumer Lag** — `kafka_consumergroup_current_offset` minus `kafka_log_latest_offset` (scraped via Pushgateway from `cdc_processor`)
2. **Rows Written per Table** — `cdc_rows_written_total{table="..."}`
3. **Last Run Timestamp** — `cdc_last_run_timestamp_seconds{table="..."}` (displayed as time-since)
4. **Processing Duration** — `cdc_processing_duration_seconds{table="..."}`

> [!NOTE]
> Kafka consumer lag will be pushed from inside `cdc_processor.py` using the Kafka Python client to compute `log_end_offset - committed_offset` per partition, then aggregated per topic. No JMX needed.

---

### 3. `spark/cdc_processor.py` — Instrument with Prometheus metrics

#### [MODIFY] [cdc_processor.py](file:///d:/DE_Project/DatebayoZenith/spark/cdc_processor.py)

Add `prometheus_client` push calls. Metrics pushed **per table** after each `process_table()` call, plus a Kafka lag push at the start of `main()`.

**New metrics pushed per run:**

| Metric name | Type | Labels | Meaning |
|-------------|------|--------|---------|
| `cdc_rows_written_total` | Gauge | `table` | Deduplicated rows written in this run |
| `cdc_processing_duration_seconds` | Gauge | `table` | Wall-clock seconds for each table |
| `cdc_last_run_timestamp_seconds` | Gauge | `table` | Unix epoch of when table was processed |
| `cdc_kafka_consumer_lag` | Gauge | `table`, `partition` | Consumer lag per topic/partition |

`process_table()` will return the row count and duration so `main()` can push them all together.

---

### 4. `spark/requirements.txt` — Add `prometheus_client` and `kafka-python`

#### [MODIFY] [requirements.txt](file:///d:/DE_Project/DatebayoZenith/spark/requirements.txt)

```
boto3==1.34.0
prometheus_client==0.20.0
kafka-python==2.0.2
```

`kafka-python` is used only to read Kafka consumer group offsets (not for consuming messages).

---

## Verification Plan

### Automated
- `docker compose up -d` — all 3 new containers start healthy
- `curl http://localhost:9091/metrics` — Pushgateway alive
- `curl http://localhost:9090/api/v1/targets` — Prometheus sees Pushgateway as `UP`
- After one Spark run (~3 min): `curl "http://localhost:9090/api/v1/query?query=cdc_rows_written_total"` — returns non-empty result

### Manual / Browser
- `localhost:3000` → Grafana → dashboard "CDC Pipeline" visible with live data
- All 4 tables show metrics updating every ~60 s (set `SLEEP_INTERVAL=60` for demo)
- Kafka lag panel shows 0 or near-0 after a run

---

## Open Questions

> [!IMPORTANT]
> **Kafka consumer group name** — the Debezium connector plus the S3 Sink Connector use different consumer groups. The `cdc_processor.py` Spark job itself does **not** consume from Kafka; it reads files that the S3 Sink already wrote.  
> Should the Kafka lag metric represent: **(a)** the S3 Sink connector's lag (how far behind the Sink is from the broker), or **(b)** a conceptual "how many CDC events have not yet been processed by Spark"?  
> **Default plan**: measure S3 Sink consumer group lag — consumer group name is `cdc-connect-group` (already in Docker Compose). Please confirm.

> [!NOTE]
> Grafana default credentials will be **admin / admin** (prompted to change on first login). If you want a custom password, let me know and I'll add a `GF_SECURITY_ADMIN_PASSWORD` env var.
