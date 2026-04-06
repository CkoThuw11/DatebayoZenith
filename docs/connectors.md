# 🔌 Connector Configuration Guide

This document explains in detail each field in the two Kafka Connect connector configuration files located in the `connectors/` directory.

---

## Overview

| File | Connector | Role |
|---|---|---|
| `debezium-postgres.json` | Debezium PostgreSQL Source | Reads WAL from Postgres, pushes events to Kafka |
| `s3-sink-minio-production.json` | Confluent S3 Sink | Reads Kafka topics, writes Avro files to MinIO |

---

## 1. Debezium PostgreSQL Source Connector

**File**: [`connectors/debezium-postgres.json`](../connectors/debezium-postgres.json)

```json
{
  "name": "source-postgres-debezium",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres-db",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "northwind",
    "database.server.name": "northwind",
    "plugin.name": "pgoutput",
    "publication.autocreate.mode": "all_tables",
    "replica.identity.autoset.values": "public.*:FULL",
    "schema.include.list": "public",
    "table.include.list": "public.orders,public.order_details,public.products,public.customers",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "topic.prefix": "northwind"
  }
}
```

### Field Explanations

#### Basic Information

| Field | Value | Explanation |
|---|---|---|
| `name` | `source-postgres-debezium` | Unique name for the connector (used in REST API). Follows contract: `source-postgres-{name}` |
| `connector.class` | `...PostgresConnector` | Java class of the Debezium plugin |
| `tasks.max` | `1` | Number of parallel tasks. For CDC, **must be set to `1`** — the WAL stream must be strictly ordered |

#### Database Connection

| Field | Value | Explanation |
|---|---|---|
| `database.hostname` | `postgres-db` | Hostname within the Docker network (service name in docker-compose) |
| `database.port` | `5432` | Default PostgreSQL port |
| `database.user` | `postgres` | User with `REPLICATION` privileges |
| `database.password` | `postgres` | Password (should use environment variables in production) |
| `database.dbname` | `northwind` | Name of the database to monitor |
| `database.server.name` | `northwind` | **Logical namespace** — used as a prefix for Kafka topic names |

#### CDC Configuration

| Field | Value | Explanation |
|---|---|---|
| `plugin.name` | `pgoutput` | WAL reading plugin. `pgoutput` is native to Postgres 10+ and **does not require extra installation** |
| `publication.autocreate.mode` | `all_tables` | Debezium automatically creates a Postgres Publication for all tables in `table.include.list` |
| `replica.identity.autoset.values` | `public.*:FULL` | Automatically sets `REPLICA IDENTITY FULL` for all tables in the `public` schema. Required for `before` data in UPDATE events |
| `schema.include.list` | `public` | Monitor only the `public` schema |
| `table.include.list` | `public.orders,...` | List of tables to monitor (comma-separated, no spaces) |

#### Serialization

| Field | Value | Explanation |
|---|---|---|
| `key.converter` | `AvroConverter` | Serializes message **keys** using Avro |
| `key.converter.schema.registry.url` | `http://schema-registry:8081` | Schema Registry URL for registering key schemas |
| `value.converter` | `AvroConverter` | Serializes message **values** using Avro |
| `value.converter.schema.registry.url` | `http://schema-registry:8081` | Schema Registry URL for registering value schemas |
| `topic.prefix` | `northwind` | Prefix for topic names. Final topic name = `northwind.public.orders` |

---

## 2. S3 Sink Connector (MinIO)

**File**: [`connectors/s3-sink-minio-production.json`](../connectors/s3-sink-minio-production.json)

```json
{
  "name": "minio-s3-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "2",
    "topics": "northwind.public.orders,northwind.public.order_details,northwind.public.products,northwind.public.customers",
    "s3.region": "us-east-1",
    "s3.bucket.name": "northwind-data-lake",
    "s3.part.size": "5242880",
    "store.url": "http://minio:9000",
    "format.class": "io.confluent.connect.s3.format.avro.AvroFormat",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd",
    "partition.duration.ms": "86400000",
    "timezone": "UTC",
    "locale": "en-US",
    "flush.size": "50",
    "rotate.interval.ms": "60000"
  }
}
```

### Field Explanations

#### Basic Information

| Field | Value | Explanation |
|---|---|---|
| `name` | `minio-s3-sink-connector` | Unique name for the connector in Kafka Connect |
| `connector.class` | `...S3SinkConnector` | Java class of the Confluent S3 Sink plugin |
| `tasks.max` | `2` | Number of parallel tasks. Sink connectors can run multiple tasks as global ordering is not required |
| `topics` | (list of 4 topics) | Kafka topics to be consumed and written to S3 |

#### MinIO Connection

| Field | Value | Explanation |
|---|---|---|
| `s3.region` | `us-east-1` | MinIO doesn't strictly use regions, but the SDK requires a valid value |
| `s3.bucket.name` | `northwind-data-lake` | Pre-created MinIO bucket name (see `register-connectors.sh`) |
| `s3.part.size` | `5242880` | 5 MB — size of a single part in multipart upload. Must be ≥ 5 MB per S3 spec |
| `store.url` | `http://minio:9000` | MinIO API endpoint URL (replaces AWS S3 endpoint) |
| `storage.class` | `S3Storage` | Backend storage implementation for S3-compatible systems |

#### File Format

| Field | Value | Explanation |
|---|---|---|
| `format.class` | `AvroFormat` | Writes files in **Avro** format (preserving Kafka schema) |

> **Note**: Can be changed to `ParquetFormat` for better compatibility with query engines (Spark, Trino, Athena), but requires additional configuration.

#### Time-Based Partitioning

| Field | Value | Explanation |
|---|---|---|
| `partitioner.class` | `TimeBasedPartitioner` | Partitions files based on the event's timestamp |
| `path.format` | `'year'=YYYY/'month'=MM/'day'=dd` | Folder format. Single quotes wrap literal text |
| `partition.duration.ms` | `86400000` | 86,400,000ms = **24 hours** — creates a new folder every day |
| `timezone` | `UTC` | Timezone used to calculate the timestamp for partitioning |
| `locale` | `en-US` | Locale for date formatting |

**Result**: Files will be located at `northwind-data-lake/topics/northwind.public.orders/year=2024/month=01/day=15/`

#### Flush & Rotation

| Field | Value | Explanation |
|---|---|---|
| `flush.size` | `50` | Flush after accumulating **50 records** |
| `rotate.interval.ms` | `60000` | Or flush every **60 seconds** (60,000ms), whichever comes first |

> **Tradeoff**: Small `flush.size` → many small files (less efficient for queries). Large `flush.size` → fewer files but higher delay. A value of `50` is suitable for dev/test environments.

---

## 3. Connector Lifecycle

```
[Deploy]  → POST /connectors  → [RUNNING]
                                    ↓ Error occurs
                               [FAILED]
                                    ↓ Fix applied
                          PUT /connectors/{name}/restart → [RUNNING]
                                    ↓ No longer needed
                          DELETE /connectors/{name}  → [DELETED]
```

### Management via REST API

```bash
# List all connectors
curl http://localhost:8083/connectors

# Check connector status
curl http://localhost:8083/connectors/source-postgres-debezium/status | jq

# Restart FAILED connector
curl -X POST http://localhost:8083/connectors/source-postgres-debezium/restart

# View current configuration
curl http://localhost:8083/connectors/source-postgres-debezium/config | jq

# Delete connector
curl -X DELETE http://localhost:8083/connectors/source-postgres-debezium

# Update configuration (e.g., add a table)
curl -X PUT http://localhost:8083/connectors/source-postgres-debezium/config \
  -H "Content-Type: application/json" \
  -d '{ "table.include.list": "public.orders,...,public.new_table", ... }'
```

---

## 4. Connector Troubleshooting

| Symptom | Inspection | Solution |
|---|---|---|
| Status `FAILED` | `GET /connectors/{name}/status` → check `trace` | Check logs: `docker logs kafka-connect` |
| No messages on topic | Check if Debezium is running | Restart connector, verify Postgres WAL settings |
| Files not appearing in MinIO | Is S3 Sink `RUNNING`? | Verify MinIO credentials in `.env` |
| Schema registry conflict | Check kafka-connect logs | Delete old schema or disable schema compatibility |
