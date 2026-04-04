# CDC Northwind Pipeline

## Overview
The Northwind CDC (Change Data Capture) Pipeline is a robust, event-driven data streaming architecture designed to capture row-level changes from a source Postgres database and propagate them to a data lake in near real-time. It leverages Debezium and Apache Kafka to serve as the backbone for low-latency data replication and integration.

## Documentation

Critical architecture decisions and data contracts are stored in the `docs/` directory:

- [**Architecture Design**](docs/architecture.md): Describes the high-level end-to-end data flow. Data is captured from a Postgres source (with logical replication enabled) via a Debezium Source Connector, streamed into a Kafka Backbone, structurally validated by a Schema Registry, and ultimately written out as Parquet files into a MinIO data lake by an S3 Sink Connector.
- [**Data & Infrastructure Contracts**](docs/contracts.md): The definitive guide establishing boundaries between teams. It enforces strict naming conventions for Kafka topics (e.g., `<database.server.name>.<schema>.<table>`) and connectors, mandates the use of Avro serialization schemas with Debezium envelope properties intact (`before`, `after`, `source`, `op`, `ts_ms`), and dictates the specific date-partitioned layout within the S3 buckets structure.

## Repository Structure
- `docs/`: Architecture concepts and data contracts.
- `docker-compose.yaml`: The single source of truth for the local integrated infrastructure environment (Kafka, Schema Registry, Kafka Connect, AKHQ Web UI, MinIO, and Postgres).
- `connectors/`: Integration boundary containing configurations for Source and Sink Kafka Connect plugins.
- `scripts/`: Shared standalone initialization or management utilities.
- `postgres/`: Source database provisioning and initialization scripts.

## Getting Started
1. Review the data flow in `docs/architecture.md` and familiarize yourself with the conventions in `docs/contracts.md`.
2. Copy `.env.example` to `.env` and fill in any required local configurations (e.g., MinIO credentials).
3. Bring up the infrastructure (Requires `docker-compose`):
   ```bash
   docker-compose up -d
   ```
4. After running docker, register the CDC and Sink connectors by running:
   ```bash
   ./scripts/register-connectors.sh
   ```
5. Explore the infrastructure using the provided web interfaces:
   - **AKHQ (Kafka UI)**: [http://localhost:8080/](http://localhost:8080/)
   - **MinIO (S3 UI)**: [http://localhost:9001/](http://localhost:9001/)
