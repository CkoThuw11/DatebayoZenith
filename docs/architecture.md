# Pipeline Architecture

## High-Level Flow
1. **Source (Postgres)**: The source Northwind database with logical replication enabled.
2. **Debezium Source Connector**: Tails the Postgres WAL and pushes changes.
3. **Kafka Backbone**: The core distributed log storing CDC events as topics.
4. **Schema Registry**: Enforces Avro schema structures on all CDC topics.
5. **S3 Sink Connector**: Consumes Kafka topics and flushes them as Parquet files to MinIO.

## Component Ownership
- **Source Team**: Postgres, Logical Replication setup, Debezium configuration (`connectors/`).
- **Backbone Team**: Kafka brokers, Schema Registry, AKHQ, network configs (`docker-compose.yaml`).
- **Sink Team**: MinIO structure, Parquet format config, S3 Sink configuration (`connectors/`).
