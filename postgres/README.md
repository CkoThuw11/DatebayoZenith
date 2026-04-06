# 🗄️ Postgres — CDC Source Database

**Owner**: Source Team

---

## Overview

This directory contains initialization scripts for the **source database** of the pipeline. PostgreSQL is configured with **Logical Replication** so that Debezium can read the WAL and capture all data changes in real-time.

The database uses the **Northwind** schema — a classic simulated commercial dataset.

---

## CDC Support Requirements

The Postgres instance must be configured with the following parameters (already set in `docker-compose.yaml`):

| Parameter | Required Value | Reason |
|---|---|---|
| `wal_level` | `logical` | Enables Logical Decoding — necessary for Debezium to read WAL |
| `max_replication_slots` | ≥ 1 | Number of replication slots (each Debezium connector uses 1 slot) |
| `max_wal_senders` | ≥ 1 | Number of WAL sender connections (each slot needs 1 sender) |
| User permission | `REPLICATION` | The connecting user must have `REPLICATION` privileges |

---

## Tables Monitored by CDC

Debezium is currently configured to monitor **4 tables** in the `public` schema:

| Table | Description | Kafka Topic |
|---|---|---|
| `public.orders` | Customer orders | `northwind.public.orders` |
| `public.order_details` | Order item details | `northwind.public.order_details` |
| `public.products` | Product catalog | `northwind.public.products` |
| `public.customers` | Customer information | `northwind.public.customers` |

### Key Columns of Interest

**`orders`**:
- `order_id` (PK) — Used as the Avro **key**
- `customer_id`, `employee_id`, `order_date`, `ship_city`

**`order_details`**:
- `order_id` + `product_id` (Composite PK)
- `unit_price`, `quantity`, `discount`

**`products`**:
- `product_id` (PK)
- `product_name`, `unit_price`, `units_in_stock`

**`customers`**:
- `customer_id` (PK, VARCHAR 5)
- `company_name`, `contact_name`, `country`

---

## Replica Identity Mechanism

By default, PostgreSQL only records the `after` value in the WAL during an UPDATE. With this configuration:

```sql
ALTER TABLE public.orders REPLICA IDENTITY FULL;
```

Postgres will record **both `before` and `after`** — allowing downstream systems to know the data **before the change**.

Debezium automatically handles this via the parameter:
```json
"replica.identity.autoset.values": "public.*:FULL"
```

---

## Local Initialization

When running `docker-compose up`, the `init.sql` file will be automatically executed to create the entire Northwind schema and seed sample data:

```bash
# Initialize database (automatic with docker-compose up)
docker-compose up -d postgres-db

# Verify database is ready
docker exec postgres-db psql -U postgres -d northwind -c "\dt public.*"
```

**Expected results** — list of Northwind tables:
```
            List of relations
 Schema |       Name        | Type  |  Owner   
--------+-------------------+-------+----------
 public | categories        | table | postgres
 public | customers         | table | postgres
 public | employees         | table | postgres
 public | order_details     | table | postgres
 public | orders            | table | postgres
 public | products          | table | postgres
 ...
```

---

## Connecting to Postgres

```bash
# Using psql inside the container
docker exec -it postgres-db psql -U postgres -d northwind

# Using an external client (DBeaver, TablePlus, ...)
Host:     localhost
Port:     5432
Database: northwind
User:     postgres
Password: postgres
```
