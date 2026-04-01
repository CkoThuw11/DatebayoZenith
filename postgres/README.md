# Postgres Source Details

**Owner**: Source Team

## Overview
This folder contains the initialization scripts and documentation for the source database. Since the database is not included in the main `docker-compose.yaml` (managed externally or via a separate process), this serves as the local development reference.

## Requirements
To support Debezium CDC, the Postgres instance must have:
- `wal_level=logical`
- A user with `REPLICATION` privileges
- The `pgoutput` plugin enabled plugin.

## Initialize
Use `init.sql` to populate the dummy Northwind schema if testing locally.
