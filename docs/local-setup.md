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
| Free RAM | ≥ 4 GB | Kafka + Connect take ~2 GB |
| Free Disk | ≥ 3 GB | Images + data volumes |

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
[+] Running 6/6
 ✔ Container kafka            Started
 ✔ Container schema-registry  Started
 ✔ Container kafka-connect    Started
 ✔ Container akhq             Started
 ✔ Container minio            Started
 ✔ Container postgres-db      Started
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

### 5c. Check Files in MinIO

1. Open http://localhost:9001
2. Login: `admin` / `admin123`
3. Enter bucket `northwind-data-lake`
4. Browse the path `topics/northwind.public.orders/year=.../month=.../day=.../`
5. You should see `.avro` files after the connector flushes (max 60 seconds or 50 records).

---

## Web UI Endpoints Summary

| Interface | URL | Login |
|---|---|---|
| AKHQ (Kafka UI) | http://localhost:8080 | None required |
| MinIO Console | http://localhost:9001 | `admin` / `admin123` |
| Kafka Connect REST | http://localhost:8083 | None required |
| Schema Registry | http://localhost:8081 | None required |

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
