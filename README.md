# 🌀 DatebayoZenith — CDC Northwind Pipeline

![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-7.4.0-231F20?logo=apachekafka&logoColor=white)
![Debezium](https://img.shields.io/badge/Debezium-2.4.2-FF0000?logo=apacheKafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-C72E49?logo=minio&logoColor=white)

> **Complete CDC (Change Data Capture) Pipeline** — automatically captures every data change from PostgreSQL and streams it in real-time to a MinIO Data Lake via Apache Kafka.

---

## 📐 Architecture Overview

![CDC Pipeline Architecture: PostgreSQL → Debezium → Kafka → Schema Registry → S3 Sink → MinIO](docs/assets/architecture.png)

**Data Flow**: `PostgreSQL` → `Debezium` → `Kafka` ↔ `Schema Registry` → `S3 Sink` → `MinIO`

---

## 📚 Documentation

All technical documentation is located in the `docs/` directory:

| Document | Description |
|---|---|
| [📐 Architecture](docs/architecture.md) | System architecture, detailed data flow, and design decisions |
| [📋 Contracts](docs/contracts.md) | Naming conventions, Avro schema, S3 layout — commitments between teams |
| [🔌 Connectors](docs/connectors.md) | Detailed explanation of each field in the Kafka Connect configuration |
| [🚀 Local Setup](docs/local-setup.md) | Step-by-step guide to running the pipeline on a local machine |

---

## 🗂️ Repository Structure

```
DatebayoZenith/
│
├── 📄 README.md                      # Overview documentation (this file)
├── 📄 docker-compose.yaml            # Local infrastructure definition
├── 📄 .env.example                   # Environment variables template
├── 📄 northwind.sql                  # Northwind sample data (backup)
│
├── 📁 docs/                          # Architecture & data contract documentation
│   ├── architecture.md               # End-to-end system design
│   ├── contracts.md                  # Data & Infrastructure contracts
│   ├── connectors.md                 # Connector configuration guide
│   └── local-setup.md                # Local setup guide
│
├── 📁 connectors/                    # Kafka Connect plugin configurations
│   ├── debezium-postgres.json        # Source connector (CDC from Postgres)
│   └── s3-sink-minio-production.json # Sink connector (writes to MinIO)
│
├── 📁 scripts/                       # Utility scripts
│   └── register-connectors.sh        # Register connectors via REST API
│
└── 📁 postgres/                      # Source database initialization
    ├── init.sql                      # Northwind schema + seed data
    └── README.md                     # Postgres CDC configuration guide
```

---

## ⚡ Quick Start

### System Requirements

| Tool | Minimum Version |
|---|---|
| Docker | 24.x or higher |
| Docker Compose | 2.x or higher |
| curl | Any (used to register connectors) |
| Bash | Git Bash / WSL / Linux / macOS |

### Startup Steps

**Step 1 — Prepare environment configuration**
```bash
cp .env.example .env
# Edit .env if you need to change credentials
```

**Step 2 — Start the entire infrastructure**
```bash
docker-compose up -d
```
> ⏳ The first run will take 5–10 minutes to download images and install connector plugins.

**Step 3 — Verify all containers are running**
```bash
docker-compose ps
```
Expected result: all services are in the `Up` state.

**Step 4 — Register Kafka Connect Connectors**
```bash
# Run from the project root directory
bash scripts/register-connectors.sh
```

**Step 5 — Access Web UIs for verification**

| Interface | URL | Purpose |
|---|---|---|
| **AKHQ** (Kafka UI) | http://localhost:8080 | View topics, messages, consumer groups |
| **MinIO** (S3 UI) | http://localhost:9001 | Browse files in the data lake |
| **Kafka Connect REST** | http://localhost:8083 | Manage connectors via API |
| **Schema Registry** | http://localhost:8081 | View registered Avro schemas |

> See more detailed instructions at [docs/local-setup.md](docs/local-setup.md).

---

## 🔧 Common Troubleshooting

| Symptom | Cause | Solution |
|---|---|---|
| `kafka-connect` restarts continuously | Downloading connector plugins | Wait 3–5 minutes, plugins need to download |
| Script reports connection refused | Kafka Connect not ready yet | Script automatically waits — be patient |
| MinIO bucket cannot be created | Credentials mismatch | Check `MINIO_ROOT_USER/PASSWORD` in `.env` |
| Connector in `FAILED` state | Postgres logical replication not enabled | Check `wal_level=logical` in docker-compose |

---

## 📖 Further Reading

- Detailed Architecture → [docs/architecture.md](docs/architecture.md)
- Conventions and Data Contracts → [docs/contracts.md](docs/contracts.md)
- Connector Configuration → [docs/connectors.md](docs/connectors.md)
