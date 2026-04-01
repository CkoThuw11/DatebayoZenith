# Data & Infrastructure Contracts

This document strictly defines the boundaries and agreements between teams.

## 1. Naming Conventions
- **Topics**: `<database.server.name>.<schema>.<table>` (e.g., `nw_logical.public.customers`)
- **Connectors**: `<type>-<system>-<name>` (e.g., `source-postgres-customers`, `sink-s3-analytics`)

## 2. Payload Structure (Schema Registry)
- All CDC payloads MUST be in Avro format.
- Keys must enforce the primary key of the source tables.
- Values must include the Debezium envelope (`before`, `after`, `source`, `op`, `ts_ms`).

## 3. MinIO S3 Layout
- Bucket Name: `northwind-datalake`
- Path Structure: `/topics/<topic_name>/year=YYYY/month=MM/day=DD/`

## 4. Integration Boundaries
- The **Source Team** is responsible for writing valid connector configs that align with the defined topic conventions.
- The **Sink Team** expects exactly these topics to be present in Schema Registry for downstream digestion.
- The **Backbone Team** guarantees the uptime of `kafka` (9092) and `schema-registry` (8081).
