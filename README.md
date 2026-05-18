# NorthStream


> **Complete CDC (Change Data Capture) Pipeline** — automatically captures every data change from PostgreSQL, streams it to MinIO via Kafka, processes Avro→Parquet with Spark, exposes it for SQL analytics via Trino, and monitors end-to-end pipeline health via Prometheus + Grafana with PagerDuty alerting.

---

## 📐 Architecture Overview

![CDC Pipeline Architecture: PostgreSQL → Debezium → Kafka → Schema Registry → S3 Sink → MinIO](imgs/Architecture.png)


---

## 🛤️ Data Lineage

```mermaid
flowchart LR
    %% Định nghĩa các style
    classDef db fill:#336791,stroke:#fff,stroke-width:2px,color:#fff;
    classDef kafka fill:#231F20,stroke:#fff,stroke-width:2px,color:#fff;
    classDef storage fill:#C72E49,stroke:#fff,stroke-width:2px,color:#fff;
    classDef process fill:#E25A1C,stroke:#fff,stroke-width:2px,color:#fff;
    classDef query fill:#DD00A1,stroke:#fff,stroke-width:2px,color:#fff;

    %% Data Sources
    subgraph Source ["1. Source System"]
        Gen([Python Generator])
        PG[(PostgreSQL)]:::db
        Gen -- "INSERT/UPDATE" --> PG
    end

    %% Ingestion
    subgraph Ingestion ["2. CDC & Streaming"]
        Debezium[Debezium Connector]
        Kafka{Apache Kafka\n(Topic)}:::kafka
        S3Sink[S3 Sink Connector]
        
        PG -- "WAL Changes" --> Debezium
        Debezium -- "Avro" --> Kafka
        Kafka -- "Consume" --> S3Sink
    end

    %% Data Lake & Processing
    subgraph DataLake ["3. Data Lake & Processing"]
        MinIORaw[(MinIO\nRaw: Avro)]:::storage
        Spark[Apache Spark\n+ Deequ DQ]:::process
        MinIOProc[(MinIO\nProcessed: Parquet)]:::storage
        
        S3Sink -- "Write Files" --> MinIORaw
        MinIORaw -- "Read Batch" --> Spark
        Spark -- "Write Transformed" --> MinIOProc
    end

    %% Analytics
    subgraph Analytics ["4. Analytics"]
        Trino[Trino Engine]:::query
        User((Data Analyst))
        
        MinIOProc -. "External Table" .- Trino
        User -- "SQL" --> Trino
    end
```

---

## 🏗️ Infrastructure

| Service | Role |
|---|---|
| `Kafka` | KRaft broker + controller |
| `Schema Registry` | Avro schema registry |
| `Akhq` | Kafka UI |
| `MinIO` | Object storage (S3-compatible) |
| `PostgreSQL` | Northwind source database |
| `Spark` | Avro → Parquet incremental processor |
| `Trino` | SQL query engine |
| `Prometheus` | Scrapes and stores metrics |
| `Grafana` | Dashboards and alerting |
| `PagerDuty` | Trigger Alert |
| `Deequ` | Data Quality |

---

## 🗂️ Repository Structure

```
DatebayoZenith/
│
├── 📄 README.md                        # Overview documentation (this file)
├── 📄 docker-compose.yaml              # Local infrastructure definition
├── 📄 .env.example                     # Environment variables template
├── 📄 register-connectors.sh           # Register connectors via REST API
│
├── 📁 docs/                            # Architecture & data contract documentation
│   ├── architecture.md
│   ├── contracts.md
│   └── connectors.md
│
├── 📁 connectors/                      # Kafka Connect plugin configurations
│   ├── debezium-postgres.json          # Source connector (CDC from Postgres)
│   └── s3-sink-minio-production.json   # Sink connector (writes Avro to MinIO)
│
├── 📁 postgres/                        # Source database initialization
│   ├── init.sql                        # Northwind schema + seed data
│   └── README.md
│
├── 📁 generator/                       # 🔄 Simulated Data Generator
│   ├── Dockerfile
│   ├── data_generator.py               # Inserts orders, updates products every 10s
│   └── requirements-generator.txt
│
├── 📁 spark/                           # ⚡ Spark CDC Processing Engine
│   ├── Dockerfile
│   ├── entrypoint.sh                   # Init tables → scheduler loop (every 60s)
│   ├── requirements.txt
│   ├── spark-config/
│   │   ├── hive-site.xml               # Points Spark to metastore
│   │   └── core-site.xml               # S3A config for MinIO access
│   └── spark-app/
│       ├── init_hive.py                # Creates database + external tables
│       ├── cdc_processor.py            # Avro→Parquet: checkpoint, dedupe, DQ, write
│       ├── deequ_checks.py             # DQ checks: non-empty, null PK, unique PK, freshness
│       ├── alert.py                    # PagerDuty Events API v2 integration
│       └── test_dq_alert.py            # End-to-end DQ + alerting test (5 scenarios)
│
├── 📁 trino/                           # 🔍 Trino SQL Query Engine
│   └── etc/
│       ├── config.properties
│       ├── jvm.config
│       ├── node.properties
│   └── catalog/
│       └── hive.properties         # Connector → MinIO
│
└── 📁 monitoring/                      # 📊 Observability Stack
    ├── prometheus.yml                  # Scrape config (pushgateway + kafka-exporter)
    └── grafana/
        ├── datasources/
        │   └── prometheus.yaml         # Grafana → Prometheus datasource
        ├── dashboards/
        │   ├── dashboard.yaml          # Dashboard provider config
        │   └── cdc_pipeline.json       # CDC Pipeline Monitoring dashboard
        └── alerting/
            └── alerting.yaml           # Alert rules (lag, freshness, Spark health)
```

---

## 🚀 Pipeline Workflow — How to Run

### Step 1 — Prepare Environment Configuration

```bash
cp .env.example .env
# Edit .env if you need to change default credentials
```

Default credentials in `.env.example`:

| Variable | Default |
|---|---|
| `MINIO_ROOT_USER` | `admin` |
| `MINIO_ROOT_PASSWORD` | `admin123` |
| `PG_USER` | `postgres` |
| `PG_PASSWORD` | `postgres` |
| `PAGERDUTY_ROUTING_KEY` | *(empty — alerts log only until set)* |

### Step 2 — Start the Entire Infrastructure

```bash
docker compose up -d --build
```

### Step 3 — Verify All Containers Are Running

```bash
docker compose ps
```

### Step 4 — Register Kafka Connect Connectors

```bash
bash register-connectors.sh
```

### Step 5 — Query Data with Trino

```bash
docker exec -it trino trino
```

```sql
-- List schemas
SHOW SCHEMAS FROM hive;

-- Switch to northwind
USE hive.northwind;

-- List tables
SHOW TABLES;

-- Query orders
SELECT * FROM hive.northwind.orders ORDER BY order_date DESC LIMIT 20;
```

### Step 7 — Access Web UIs

| Interface | URL | Purpose |
|---|---|---|
| **AKHQ** (Kafka UI) | http://localhost:8090 | View topics, messages, consumer groups |
| **MinIO** (S3 UI) | http://localhost:9001 | Browse Avro & Parquet files |
| **Trino UI** | http://localhost:8080 | SQL query monitoring & analytics |
| **Grafana** | http://localhost:3000 | CDC Pipeline dashboard + alert rules |
---
