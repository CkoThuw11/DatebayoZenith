# 📋 Data & Infrastructure Contracts

This document defines the **boundaries and commitments** between teams. Every member working with the pipeline **must comply** with the following conventions.

> ⚠️ **Note**: Any changes violating these contracts may cause **pipeline failures** or **downstream data loss**.

---

## 1. Naming Conventions

### Kafka Topics

**Pattern**: `{topic.prefix}.{schema}.{table}`

| Component | Current Value | Notes |
|---|---|---|
| `topic.prefix` | `northwind` | Database server name, configured in Debezium |
| `schema` | `public` | PostgreSQL schema |
| `table` | table name | Lowercase, uses `_` |

**List of Current Topics**:

| Topic | Source Table |
|---|---|
| `northwind.public.orders` | `public.orders` |
| `northwind.public.order_details` | `public.order_details` |
| `northwind.public.products` | `public.products` |
| `northwind.public.customers` | `public.customers` |

---

### Kafka Connect Connectors

**Pattern**: `{type}-{system}-{name}`

| Type | System | Name | Full Name |
|---|---|---|---|
| `source` | `postgres` | `debezium` | `source-postgres-debezium` |
| `sink` | `s3` | `minio-connector` | `minio-s3-sink-connector` |

---

### MinIO Buckets

| Bucket | Purpose |
|---|---|
| `northwind-data-lake` | Primary bucket containing all CDC data |

---

## 2. Payload Structure — Avro & Schema Registry

### Mandatory Principles

- ✅ **All** messages on Kafka topics MUST be serialized using **Avro**
- ✅ **Keys** MUST contain the **primary key** of the source table
- ✅ **Values** MUST retain the full **Debezium envelope**

---

### Debezium Envelope — Value Structure

Each message value on Kafka follows the standard Debezium envelope structure:

```json
{
  "before": { ... },
  "after":  { ... },
  "source": { ... },
  "op":     "c | u | d | r",
  "ts_ms":  1700000000000
}
```

| Field | Type | Meaning |
|---|---|---|
| `before` | Object \| null | Snapshot of data **before** the change. `null` for INSERTs |
| `after` | Object \| null | Snapshot of data **after** the change. `null` for DELETEs |
| `source` | Object | Source metadata: DB name, table name, WAL LSN, timestamp |
| `op` | String | Operation type: `c`=CREATE, `u`=UPDATE, `d`=DELETE, `r`=READ (snapshot) |
| `ts_ms` | Long | Timestamp (milliseconds) when Debezium processed the event |

---

### Example Avro Schema — `orders` table

**Key Schema** (`northwind.public.orders-key`):
```json
{
  "type": "record",
  "name": "Key",
  "namespace": "northwind.public.orders",
  "fields": [
    { "name": "order_id", "type": "int" }
  ]
}
```

**Value Schema** (`northwind.public.orders-value`) — simplified:
```json
{
  "type": "record",
  "name": "Envelope",
  "namespace": "northwind.public.orders",
  "fields": [
    {
      "name": "before",
      "type": ["null", {
        "type": "record",
        "name": "Value",
        "fields": [
          { "name": "order_id",   "type": "int" },
          { "name": "customer_id","type": ["null", "string"] },
          { "name": "order_date", "type": ["null", { "type": "int", "logicalType": "date" }] }
        ]
      }],
      "default": null
    },
    { "name": "after",   "type": ["null", "Value"], "default": null },
    {
      "name": "source",
      "type": {
        "type": "record",
        "name": "Source",
        "fields": [
          { "name": "db",     "type": "string" },
          { "name": "schema", "type": "string" },
          { "name": "table",  "type": "string" },
          { "name": "lsn",    "type": ["null", "long"], "default": null }
        ]
      }
    },
    { "name": "op",    "type": "string" },
    { "name": "ts_ms", "type": ["null", "long"], "default": null }
  ]
}
```

---

### Example Real-world Message

**Event: UPDATE on order `order_id = 10248`**

```json
{
  "before": {
    "order_id": 10248,
    "customer_id": "VINET",
    "order_date": 9225
  },
  "after": {
    "order_id": 10248,
    "customer_id": "VINET",
    "order_date": 9226
  },
  "source": {
    "db": "northwind",
    "schema": "public",
    "table": "orders",
    "lsn": 24123456
  },
  "op": "u",
  "ts_ms": 1700000512345
}
```

---

## 3. MinIO S3 Data Lake Structure

### Folder Layout

```
northwind-data-lake/                          ← Bucket
├── topics/                                   ← S3 Sink default prefix (Avro)
│   ├── northwind.public.orders/
│   │   └── year=2026/
│   │       └── month=04/
│   │           └── day=09/
│   │               ├── northwind.public.orders+0+0000000000.avro
│   │               └── northwind.public.orders+0+0000000050.avro
│   ├── northwind.public.order_details/
│   ├── northwind.public.products/
│   └── northwind.public.customers/
│
└── parquet/                                  ← Spark output prefix (Parquet)
    ├── orders/
    │   └── year=2026/month=04/day=09/
    │       └── part-00000-*.snappy.parquet
    ├── order_details/
    ├── products/
    └── customers/
```

### File Naming Convention

**Pattern**: `{topic}+{partition}+{offset}.avro`

| Part | Example | Explanation |
|---|---|---|
| `topic` | `northwind.public.orders` | Kafka topic name |
| `partition` | `0` | Kafka partition number |
| `offset` | `0000000050` | First offset in the file (10 digits, zero-padded) |

---

## 4. Parquet Layer Contract (Spark → Trino)

This section defines the interface between the **Spark CDC Engine** (writer) and **Trino** (reader). Both components must adhere to these constraints.

### Output Path

```
s3a://northwind-data-lake/parquet/<table>/year=YYYY/month=MM/day=DD/
```

### File Format Requirements

| Property | Value | Notes |
|---|---|---|
| Format | Parquet | Columnar, efficient for analytics |
| Compression | Snappy | Balanced between speed and size |
| Partitioning | Hive-style: `year=X/month=XX/day=XX` | Enables partition pruning in Trino |
| Deduplication | One row per primary key | Last-Write-Wins by `cdc_ts_ms DESC` |

### Required Columns (all tables)

In addition to all source table columns, every Parquet file **must** include:

| Column | Type | Description |
|---|---|---|
| `cdc_op` | VARCHAR | CDC event type: `c` create, `u` update, `r` read/snapshot |
| `cdc_ts_ms` | BIGINT | Debezium timestamp in milliseconds |
| `year` | VARCHAR | Partition column (UTC, zero-padded) |
| `month` | VARCHAR | Partition column (UTC, zero-padded) |
| `day` | VARCHAR | Partition column (UTC, zero-padded) |

### Critical Type Mappings

These types are **intentionally different** from the Postgres source types to avoid issues with Spark/Trino serialization:

| Column | Postgres | Parquet / Trino | Reason |
|---|---|---|---|
| `order_date`, `required_date`, `shipped_date` | `DATE` | VARCHAR | Avro DATE is serialized as integer epoch days; stored as ISO string to avoid timezone errors |
| `quantity` (order_details) | `SMALLINT` | INTEGER | Python `int` serializes to INT64 in Parquet |
| `units_in_stock`, `units_on_order`, `reorder_level` | `SMALLINT` | INTEGER | Same reason |
| `discontinued` | `BOOLEAN` | INTEGER | Stored as 0 or 1 |

### Post-Write Sync Requirement

> ⚠️ **Important**: After every Spark run, the following commands **must** be executed in Trino to make new partitions visible:

```sql
CALL hive.system.sync_partition_metadata('northwind', 'orders',        'FULL');
CALL hive.system.sync_partition_metadata('northwind', 'order_details', 'FULL');
CALL hive.system.sync_partition_metadata('northwind', 'products',      'FULL');
CALL hive.system.sync_partition_metadata('northwind', 'customers',     'FULL');
```

This is required because the Hive file metastore does not automatically detect new Hive-style partition directories.

---

## 5. Schema Evolution Policy

| Change Type | Allowed? | Notes |
|---|---|---|
| Add new column (nullable) | ✅ Yes | Must have `default: null` in Avro schema |
| Delete column | ❌ No | Breaking change — requires migration plan |
| Rename column | ❌ No | Breaking change — requires alias |
| Data type change | ❌ No | Breaking change — requires versioning |
| Add new table to CDC | ✅ Yes | Update `table.include.list` in Debezium config |

---

## 6. Contract Change Process

When a contract needs to be changed:

1. **Notify** all relevant teams at least **1 week** in advance
2. **Create a branch** specifically for migration
3. **Verify** backward compatibility of Avro schema before applying
4. **Update** this document concurrently with the code change
5. **Do not merge** until downstream teams confirm readiness