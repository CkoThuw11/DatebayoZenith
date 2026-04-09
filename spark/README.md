# ⚡ Spark CDC Processing Engine — Avro → Parquet

This engine automatically processes CDC (Change Data Capture) events from Debezium: reads Avro files from MinIO, deduplicates records, converts them to Parquet with Hive-style partitioning, and makes the data queryable by Trino.

---

## Data Flow

```
MinIO (Avro — Debezium CDC envelope)
  s3a://northwind-data-lake/topics/northwind.public.<table>/
          │
          ▼
  Spark CDC Processor  (runs every 3 minutes)
    1. Read all Avro files (recursiveFileLookup = true)
    2. Filter: op IN ('c', 'u', 'r')  AND  after IS NOT NULL
    3. Flatten 'after' struct → top-level columns
    4. Rename: ts_ms → cdc_ts_ms,  op → cdc_op
    5. Deduplicate: Last-Write-Wins per Primary Key (order by cdc_ts_ms DESC)
    6. Add partition columns: year / month / day  (UTC)
    7. Write Parquet (Snappy compressed, Hive-style partitioning)
          │
          ▼
  MinIO (Parquet — clean, deduplicated)
  s3a://northwind-data-lake/parquet/<table>/year=YYYY/month=MM/day=DD/
          │
          ▼
      Trino  (SQL queries on port 8090)
```

---

## Directory Structure

```
spark/
├── Dockerfile           # Based on apache/spark:3.5.1-python3 with pinned JARs
├── entrypoint.sh        # Scheduler loop — calls spark-submit every N seconds
├── cdc_processor.py     # Core logic: read Avro → deduplicate → write Parquet
├── requirements.txt     # Python dependencies: boto3==1.34.0
└── README.md            # This file
```

---

## Configuration

All configuration is injected via **environment variables** in `docker-compose.yaml`:

| Variable | Default | Description |
|---|---|---|
| `MINIO_ROOT_USER` | `minioadmin` | MinIO access key |
| `MINIO_ROOT_PASSWORD` | `minioadmin123` | MinIO secret key |
| `MINIO_ENDPOINT` | `http://minio:9000` | Internal MinIO endpoint |
| `S3_BUCKET` | `northwind-data-lake` | Bucket name containing all data |
| `SLEEP_INTERVAL` | `180` | Processing cycle in seconds. **180s = 3 minutes** (within the 3–5 min requirement) |

### Tables & Primary Keys

| Table | Primary Key |
|---|---|
| `orders` | `order_id` |
| `order_details` | `order_id`, `product_id` |
| `products` | `product_id` |
| `customers` | `customer_id` |

---

## JAR Dependencies (pinned at build time)

| JAR | Version | Purpose |
|---|---|---|
| `spark-avro_2.12` | 3.5.1 | Read Avro format |
| `hadoop-aws` | 3.3.4 | S3A FileSystem for MinIO |
| `aws-java-sdk-bundle` | 1.12.262 | AWS SDK required by hadoop-aws |

> ✅ All JARs are downloaded at **build time** — no internet access required at runtime.

---

## Running

### Option 1 — Docker Compose (recommended)

The `spark-cdc` service is already declared in `docker-compose.yaml`:

```bash
# From the project root
docker-compose up -d spark-cdc
```

Spark automatically waits 30 seconds for MinIO to become healthy, then runs every 3 minutes in a loop.

### Option 2 — Build & Run manually

```bash
# Build the image
docker build -t spark-cdc ./spark

# Run with environment variables
docker run --rm \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin123 \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e S3_BUCKET=northwind-data-lake \
  -e SLEEP_INTERVAL=180 \
  --network datebayozenith_default \
  spark-cdc
```

---

## S3 Path Layout

### Input — Avro files written by Kafka S3 Sink Connector

```
northwind-data-lake/
└── topics/
    └── northwind.public.<table>/
        └── year=YYYY/month=MM/day=DD/
            ├── northwind.public.<table>+0+0000000000.avro
            ├── northwind.public.<table>+0+0000000050.avro
            └── ...
```

### Output — Parquet files read by Trino

```
northwind-data-lake/
└── parquet/
    ├── orders/
    │   └── year=2026/month=04/day=09/
    │       └── part-00000-*.snappy.parquet
    ├── order_details/
    │   └── year=2026/month=04/day=09/
    ├── products/
    │   └── year=2026/month=04/day=09/
    └── customers/
        └── year=2026/month=04/day=09/
```

---

## Parquet Output Schema

Each Parquet file contains the following columns (example: `orders`):

| Column | Type | Source |
|---|---|---|
| `order_id` | INTEGER | `after.order_id` |
| `customer_id` | VARCHAR | `after.customer_id` |
| `employee_id` | INTEGER | `after.employee_id` |
| `order_date` | VARCHAR | `after.order_date` (stored as string to avoid timezone issues) |
| `required_date` | VARCHAR | `after.required_date` |
| `shipped_date` | VARCHAR | `after.shipped_date` (nullable) |
| `ship_via` | INTEGER | `after.ship_via` |
| `freight` | DOUBLE | `after.freight` |
| `ship_name` … `ship_country` | VARCHAR | `after.*` |
| `cdc_op` | VARCHAR | Debezium envelope `op` field (`c`/`u`/`r`) |
| `cdc_ts_ms` | BIGINT | Debezium envelope `ts_ms` field |
| `year` | VARCHAR | Partition column (UTC) |
| `month` | VARCHAR | Partition column (UTC) |
| `day` | VARCHAR | Partition column (UTC) |

---

## Monitoring & Verification

### 1. Watch Spark logs

```bash
docker logs -f spark-cdc
```

Expected output every 3 minutes:
```
[entrypoint] 2026-04-09T04:00:00Z - Starting CDC processing run...
INFO cdc_processor - Processing table 'orders' from s3a://northwind-data-lake/topics/...
INFO cdc_processor - Raw record count for 'orders': 830
INFO cdc_processor - Deduplicated record count for 'orders': 415
INFO cdc_processor - Wrote Parquet for 'orders' → s3a://...parquet/orders/ (partition: year=2026/month=04/day=09)
...
INFO cdc_processor - CDC Processor run complete.
[entrypoint] Sleeping 180s until next run...
```

### 2. Browse files in MinIO Console

Open [http://localhost:9001](http://localhost:9001) → bucket `northwind-data-lake` → `parquet/` folder.

### 3. Verify via Trino

```bash
docker exec -it trino trino
```

```sql
-- Sync partitions so Trino sees new data
CALL hive.system.sync_partition_metadata('northwind', 'orders', 'FULL');

-- Check row count
SELECT COUNT(*) FROM hive.northwind.orders;

-- Verify no duplicates exist (expected: 0 rows)
SELECT order_id, COUNT(*) AS cnt
FROM hive.northwind.orders
GROUP BY order_id
HAVING cnt > 1;
```

---

## Troubleshooting

| Symptom | Cause | Solution |
|---|---|---|
| `Could not read source path` | No Avro files in MinIO yet | Ensure S3 Sink Connector is running and topics have data |
| `ClassNotFoundException: S3AFileSystem` | Missing JAR | Rebuild image: `docker-compose build spark-cdc` |
| `Connection refused` to MinIO | MinIO not ready | Spark already waits 30s — check `docker logs minio` |
| Empty Parquet after run | All events have `op='d'` — filtered correctly | Verify there are `c/u/r` events in your topics |
| Trino shows no new data | Partition not synced | Run `CALL hive.system.sync_partition_metadata(...)` |

---

## Contract with Trino

Spark guarantees the following output contract for Trino compatibility:

- **Path**: `s3a://northwind-data-lake/parquet/<table>/`
- **Format**: Parquet, Snappy compressed
- **Partitioning**: Hive-style `year=YYYY/month=MM/day=DD`
- **Required columns**: `cdc_op` (VARCHAR), `cdc_ts_ms` (BIGINT), `year/month/day` (VARCHAR)
- **Deduplication**: Exactly one row per primary key per run (last write wins)

> ⚠️ After each Spark run, a `CALL hive.system.sync_partition_metadata(...)` is required in Trino to detect newly written partitions.

---

## Related

- **Trino README**: [`../trino/README.md`](../trino/README.md)
- **docker-compose.yaml**: [`../docker-compose.yaml`](../docker-compose.yaml)
- **Architecture docs**: [`../docs/architecture.md`](../docs/architecture.md)
- **Data contracts**: [`../docs/contracts.md`](../docs/contracts.md)
