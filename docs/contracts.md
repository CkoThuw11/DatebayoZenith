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