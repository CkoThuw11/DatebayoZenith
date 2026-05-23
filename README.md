# 🌊 NorthStream - Real-Time CDC Data Replication Pipeline

> A PoC CDC platform for replicating PostgreSQL operational data into an isolated analytics environment using Debezium, Kafka, Spark, MinIO, Trino, and Grafana.

---

# 💥 Challenge: Unsafe Debugging on Production Systems

Developers often need production-like data for debugging and testing.

However, traditional approaches are inefficient and risky:
- Data replication is slow and quickly becomes outdated
- Production incidents are difficult to reproduce
- Direct queries on production systems may impact performance and stability

Therefore, organizations need an isolated environment that provides near real-time operational data without affecting production systems.

---

# ✅ Solution

NorthStream provides a real-time CDC replication platform that continuously synchronizes PostgreSQL data into an isolated analytics environment.

---

# 🛠️ Technology Stack

| Layer | Technologies |
|---|---|
| CDC | Debezium, Kafka Connect |
| Streaming | Apache Kafka |
| Storage | MinIO |
| Processing | Apache Spark |
| Data Quality | Deequ |
| Query Engine | Trino |
| Monitoring | Prometheus, Grafana |
| Alerting | PagerDuty |
| Containerization | Docker Compose |

---

# 🌟 System Architecture

<p align="center">
  <img src="./imgs/Architecture.png" width="100%">
</p>

<p align="center">
  System Architecture
</p>


---

# 🗄️ Data Lineage

<p align="center">
  <img src="./imgs/DataLineage.png" width="100%">
</p>

<p align="center">
  Data Lineage
</p>


# 📁 Repository Structure

```shell
├── README.md
├── connectors
│   ├── debezium-postgres.json
│   └── s3-sink-minio-production.json
├── docker-compose.yaml
├── generator
│   ├── Dockerfile
│   ├── data_generator.py
│   └── requirements-generator.txt
├── imgs
│   ├── AKHQ_Topics.png
│   ├── Architecture.png
│   ├── Bronze_bucket.png
│   ├── DataLineage.png
│   ├── Monitoring.jpg
│   └── TrinoQuery.png
├── monitoring
│   ├── grafana
│   │   ├── alerting
│   │   │   └── alerting.yaml
│   │   ├── dashboards
│   │   │   ├── cdc_pipeline.json
│   │   │   └── dashboard.yaml
│   │   └── datasources
│   │       └── prometheus.yaml
│   ├── jmx
│   │   └── kafka-connect-jmx.yml
│   └── prometheus.yml
├── postgres
│   └── init.sql
├── register-connectors.sh
├── spark
│   ├── Dockerfile
│   ├── entrypoint.sh
│   ├── requirements.txt
│   ├── spark-app
│   │   ├── alert.py
│   │   ├── cdc_processor.py
│   │   ├── deequ_checks.py
│   │   ├── init_hive.py
│   │   └── test_dq_alert.py
│   └── spark-config
│       ├── core-site.xml
│       └── hive-site.xml
└── trino
    ├── catalog
    │   └── hive.properties
    └── etc
        ├── config.properties
        ├── jvm.config
        └── node.properties
```
---

# 🚀 Getting Started

1. **Clone the repository**

```bash
git clone <repository-url>
cd NorthStream
```

2. **Configure environment variables**

```bash
cp .env.example .env
```

3. **Start the infrastructure**

```bash
docker compose up -d --build
```

4. **Verify running services**

```bash
docker compose ps
```

5. **Register Kafka Connect connectors**

```bash
bash register-connectors.sh
```

The script will:
- Create MinIO buckets
- Register Debezium connector
- Register S3 Sink connector
- Validate connector status

---

# 🔄 CDC Data Flow

```text
PostgreSQL
   ↓
Debezium
   ↓
Kafka
   ↓
S3 Sink Connector
   ↓
MinIO (raw)
   ↓
Spark CDC Processor
   ↓
MinIO (bronze)
   ↓
Trino
```

---

# 🔍 Query Data with Trino

```bash
docker exec -it trino trino
```

```sql
SHOW SCHEMAS FROM hive;

USE hive.northwind;

SHOW TABLES;

SELECT *
FROM orders
LIMIT 20;
```

---

# 📊 Monitoring & Alerting

<p align="center">
  <img src="./imgs/Monitoring.jpg" width="100%">
</p>

## Data Quality Checks

| Check | Rule |
|---|---|
| Non-empty | Table must contain rows |
| Null PK | No NULL primary keys |
| Unique PK | No duplicate primary keys |
| Valid cdc_op | cdc_op ∈ {c, u, r} |
| Freshness | CDC freshness SLA |

## Alert Rules

| Alert | Condition |
|---|---|
| Kafka Consumer Lag | Lag > threshold |
| Spark Not Running | No metrics update |
| Freshness SLA Breach | CDC delay > 15 min |

---

# 🌐 Service Interfaces

| Interface | URL |
|---|---|
| AKHQ | http://localhost:8090 |
| MinIO | http://localhost:9001 |
| Trino | http://localhost:8080 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |

---

# 🖥️ User Interfaces

## AKHQ UI

<p align="center">
  <img src="./imgs/AKHQ_Topics.png" width="100%">
</p>

## MinIO Console

<p align="center">
  <img src="./imgs/Bronze_bucket.png" width="100%">
</p>

## Trino Query UI

<p align="center">
  <img src="./imgs/TrinoQuery.png" width="100%">
</p>

---
# 🙏 Acknowledgements

We would like to sincerely thank our mentor for the guidance, support, and valuable feedback throughout the development of this project.

Your mentorship helped our team better understand modern data engineering concepts and successfully build this real-time CDC pipeline system.