# 🚀 Local Pipeline Setup Guide

This document provides a step-by-step guide to starting and testing the entire CDC pipeline on a personal computer.

---

## System Requirements

| Tool | Version | Verification |
|---|---|---|
| Docker | ≥ 24.0 | `docker --version` |
| Docker Compose | ≥ 2.0 | `docker compose version` |
| curl | Any | `curl --version` |
| Bash | Any | Git Bash (Windows) / Terminal (macOS/Linux) |
| Free RAM | ≥ 6 GB | Kafka + Spark + Trino take ~4–5 GB |
| Free Disk | ≥ 5 GB | Images + data volumes + JARs |

---

## Step 1 — Clone & Prepare Configuration

```bash
# Clone repository
git clone <repo-url>
cd DatebayoZenith

# Create .env from template
cp .env.example .env
```

**Default `.env` file** (can be left as-is for local runs):
```env
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=admin123
```

---

## Step 2 — Start Infrastructure

```bash
docker-compose up -d
```

**Expected results**:
```
[+] Running 8/8
 ✔ Container kafka            Started
 ✔ Container schema-registry  Started
 ✔ Container kafka-connect    Started
 ✔ Container akhq             Started
 ✔ Container minio            Started
 ✔ Container postgres-db      Started
 ✔ Container spark-cdc        Started
 ✔ Container trino            Started
```

> ⏳ **First run** takes 5–10 minutes because `kafka-connect` needs to download and install two connector plugins (Debezium + S3 Sink). This is normal.

---

## Step 3 — Verify Container Status

```bash
docker-compose ps
```

**Expected results** — all must be in the `running` state:
```
NAME              IMAGE                                    STATUS
kafka             confluentinc/cp-kafka:7.4.0             running
schema-registry   confluentinc/cp-schema-registry:7.4.0  running
kafka-connect     confluentinc/cp-kafka-connect:7.4.0     running
akhq              tchiotludo/akhq                          running
minio             minio/minio                              running
postgres-db       postgres:15                             running
data-generator    (custom)                                running
spark-cdc         (custom)                                running
trino             trinodb/trino:435                       running
```

**Check if Kafka Connect is ready**:
```bash
curl -s http://localhost:8083/connectors
# Result: []  (empty array — no connectors registered yet)
```

If there is no response, Kafka Connect is still starting — **wait a few more minutes**.

---

## Step 4 — Register Connectors

```bash
# Run from the project root directory
bash scripts/register-connectors.sh
```

**Expected results**:
```
Waiting for Kafka Connect to be ready...
 Kafka Connect is ready!
Creating MinIO bucket...
Registering Debezium PostgreSQL connector...
{"name":"source-postgres-debezium","config":{...},"tasks":[...],"type":"source"}
Registering S3 MinIO sink connector...
{"name":"minio-s3-sink-connector","config":{...},"tasks":[...],"type":"sink"}
=========================================
All Connectors Registered!
=========================================
```

**Check connector status**:
```bash
curl -s http://localhost:8083/connectors/source-postgres-debezium/status | python -m json.tool
```

**Expected results**:
```json
{
  "name": "source-postgres-debezium",
  "connector": { "state": "RUNNING", "worker_id": "kafka-connect:8083" },
  "tasks": [{ "id": 0, "state": "RUNNING", "worker_id": "kafka-connect:8083" }],
  "type": "source"
}
```

---

## Step 5 — Verify Pipeline Operation

### 5a. Create Test Data in Postgres

```bash
# Connect to Postgres
docker exec -it postgres-db psql -U postgres -d northwind

# Insert a new order
INSERT INTO orders (order_id, customer_id, employee_id, order_date, required_date)
VALUES (99999, 'ALFKI', 1, CURRENT_DATE, CURRENT_DATE + 7);

# Update the order
UPDATE orders SET ship_city = 'Hanoi' WHERE order_id = 99999;

# Exit
\q
```

### 5b. Check Events on Kafka (AKHQ)

1. Open http://localhost:8080
2. Select **Topics** → `northwind.public.orders`
3. Select the **Messages** tab — you should see the INSERT and UPDATE events just created.

### 5c. Check Parquet files in MinIO

1. Open http://localhost:9001
2. Login: `admin` / `admin123`
3. Enter bucket `northwind-data-lake`
4. Check `topics/northwind.public.orders/year=.../month=.../day=.../` for `.avro` files
5. Check `parquet/orders/year=.../month=.../day=.../` for `.parquet` files (appears after Spark runs)

---

## Step 5 — Initialize Trino Tables

Wait for Trino to be healthy (~30 seconds after startup), then create the 4 external tables:

```bash
docker exec -it trino trino --file /init/create_tables.sql
```

Verify tables were created:
```bash
docker exec -it trino trino --execute "SHOW TABLES FROM hive.northwind;"
```

Expected output:
```
     Table
--------------
 customers
 order_details
 orders
 products
(4 rows)
```

---

## Step 6 — Verify Spark is Processing

Spark waits 30 seconds for MinIO, then begins processing. Check its progress:

```bash
docker logs -f spark-cdc
```

Expected output every 3 minutes:
```
[entrypoint] Starting CDC processing run...
INFO cdc_processor - Processing table 'orders'...
INFO cdc_processor - Raw record count for 'orders': 830
INFO cdc_processor - Deduplicated record count for 'orders': 415
INFO cdc_processor - Wrote Parquet for 'orders' → s3a://northwind-data-lake/parquet/orders/
INFO cdc_processor - CDC Processor run complete.
[entrypoint] Sleeping 180s until next run...
```

> ⏳ If Avro files have not yet been written to MinIO (Kafka Connect still starting), Spark will log `Could not read source path` and skip that table — this is expected behavior.

---

## Step 7 — Query Data with Trino

Once Spark has completed at least one run:

```bash
docker exec -it trino trino
```

```sql
-- Sync partitions so Trino sees Spark's output
CALL hive.system.sync_partition_metadata('northwind', 'orders',        'FULL');
CALL hive.system.sync_partition_metadata('northwind', 'order_details', 'FULL');
CALL hive.system.sync_partition_metadata('northwind', 'products',      'FULL');
CALL hive.system.sync_partition_metadata('northwind', 'customers',     'FULL');

-- Basic query
SELECT order_id, customer_id, ship_city, cdc_op
FROM hive.northwind.orders
ORDER BY cdc_ts_ms DESC
LIMIT 10;

-- Revenue analysis
SELECT
    o.order_id,
    o.customer_id,
    SUM(od.unit_price * od.quantity * (1 - od.discount)) AS total_revenue
FROM hive.northwind.orders o
JOIN hive.northwind.order_details od ON o.order_id = od.order_id
GROUP BY o.order_id, o.customer_id
ORDER BY total_revenue DESC
LIMIT 10;
```

---

## Web UI Endpoints Summary

| Interface | URL | Login | Description |
|---|---|---|---|
| AKHQ (Kafka UI) | http://localhost:8080 | None | Browse topics, messages, connectors |
| MinIO Console | http://localhost:9001 | `admin` / `admin123` | Browse Avro and Parquet files |
| Kafka Connect REST | http://localhost:8083 | None | Manage connector lifecycle |
| Schema Registry | http://localhost:8081 | None | View registered Avro schemas |
| Trino UI | http://localhost:8090 | None | SQL query monitoring and history |

---

## Stopping the Pipeline

```bash
# Stop all containers (retain data volumes)
docker-compose down

# Stop and remove all data (clean slate)
docker-compose down -v
```

---

## Troubleshooting

### Kafka Connect continuously restarts

```bash
docker logs kafka-connect --tail 50
```

If you see `Downloading connector...` → it is still downloading plugins, wait another 5 minutes.

---

### Connector in FAILED state

```bash
curl -s http://localhost:8083/connectors/source-postgres-debezium/status | python -m json.tool
# Check "tasks[0].trace" for the error reason

# Restart connector
curl -X POST http://localhost:8083/connectors/source-postgres-debezium/restart
```

---

### Spark shows `Could not read source path`

This means Avro files are not yet in MinIO for that table. This is **expected** while Kafka Connect is still starting or no data has been produced yet. Spark will retry on the next cycle (3 minutes).

---

### Trino returns 0 rows

Either Spark has not run yet, or partitions have not been synced:

```bash
# Check Spark has written Parquet files
docker exec minio mc ls minio/northwind-data-lake/parquet/orders/ --recursive

# Then sync partitions in Trino
docker exec -it trino trino --execute "CALL hive.system.sync_partition_metadata('northwind', 'orders', 'FULL');"
```

---

### MinIO bucket not created (Incorrect Credentials)

```bash
# Verify .env
cat .env

# Check script logs
docker exec minio mc alias set local http://localhost:9000 admin admin123
```

Ensure `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` in `.env` match the command above.

---

### Port already in use

```powershell
# Windows — Find process using port 9092
netstat -ano | findstr :9092
taskkill /PID <PID> /F
```

```bash
# macOS/Linux
lsof -i :9092
kill -9 <PID>
```
