# 🔌 Hướng Dẫn Cấu Hình Connectors

Tài liệu này giải thích chi tiết từng field trong hai file cấu hình Kafka Connect connector nằm trong thư mục `connectors/`.

---

## Tổng Quan

| File | Connector | Vai Trò |
|---|---|---|
| `debezium-postgres.json` | Debezium PostgreSQL Source | Đọc WAL từ Postgres, đẩy events vào Kafka |
| `s3-sink-minio-production.json` | Confluent S3 Sink | Đọc Kafka topics, ghi Avro files vào MinIO |

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

### Giải Thích Từng Field

#### Thông Tin Cơ Bản

| Field | Giá Trị | Giải Thích |
|---|---|---|
| `name` | `source-postgres-debezium` | Tên định danh connector (dùng trong REST API). Tuân thủ contract: `source-postgres-{name}` |
| `connector.class` | `...PostgresConnector` | Class Java của Debezium plugin |
| `tasks.max` | `1` | Số task song song. Với CDC, **phải để `1`** — stream WAL phải có thứ tự tuyệt đối |

#### Kết Nối Database

| Field | Giá Trị | Giải Thích |
|---|---|---|
| `database.hostname` | `postgres-db` | Hostname trong Docker network (tên service trong docker-compose) |
| `database.port` | `5432` | Port mặc định PostgreSQL |
| `database.user` | `postgres` | User có quyền `REPLICATION` |
| `database.password` | `postgres` | Mật khẩu (nên dùng biến môi trường ở production) |
| `database.dbname` | `northwind` | Tên database cần CDC |
| `database.server.name` | `northwind` | **Namespace logic** — dùng làm prefix cho tên Kafka topics |

#### Cấu Hình CDC

| Field | Giá Trị | Giải Thích |
|---|---|---|
| `plugin.name` | `pgoutput` | Plugin đọc WAL. `pgoutput` là plugin native của Postgres 10+, **không cần cài thêm** |
| `publication.autocreate.mode` | `all_tables` | Debezium tự tạo Postgres Publication cho tất cả bảng trong `table.include.list` |
| `replica.identity.autoset.values` | `public.*:FULL` | Tự động set `REPLICA IDENTITY FULL` cho tất cả bảng trong schema `public`. Cần thiết để có dữ liệu `before` khi UPDATE |
| `schema.include.list` | `public` | Chỉ theo dõi schema `public` |
| `table.include.list` | `public.orders,...` | Danh sách bảng cần CDC (cách nhau bởi dấu phẩy, không dấu cách) |

#### Serialization

| Field | Giá Trị | Giải Thích |
|---|---|---|
| `key.converter` | `AvroConverter` | Serialize message **key** bằng Avro |
| `key.converter.schema.registry.url` | `http://schema-registry:8081` | URL Schema Registry để đăng ký key schema |
| `value.converter` | `AvroConverter` | Serialize message **value** bằng Avro |
| `value.converter.schema.registry.url` | `http://schema-registry:8081` | URL Schema Registry để đăng ký value schema |
| `topic.prefix` | `northwind` | Prefix cho tên topic. Topic = `northwind.public.orders` |

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

### Giải Thích Từng Field

#### Thông Tin Cơ Bản

| Field | Giá Trị | Giải Thích |
|---|---|---|
| `name` | `minio-s3-sink-connector` | Tên connector trong Kafka Connect |
| `connector.class` | `...S3SinkConnector` | Class Java của Confluent S3 Sink plugin |
| `tasks.max` | `2` | Số task song song. Sink có thể chạy nhiều task vì không cần đảm bảo thứ tự global |
| `topics` | (danh sách 4 topics) | Các Kafka topics sẽ được consume và ghi vào S3 |

#### Kết Nối MinIO

| Field | Giá Trị | Giải Thích |
|---|---|---|
| `s3.region` | `us-east-1` | MinIO không thực sự dùng region, nhưng SDK yêu cầu — để bất kỳ giá trị hợp lệ nào |
| `s3.bucket.name` | `northwind-data-lake` | Tên bucket MinIO đã tạo sẵn (xem `register-connectors.sh`) |
| `s3.part.size` | `5242880` | 5 MB — Kích thước một phần trong multipart upload. Phải ≥ 5 MB theo S3 spec |
| `store.url` | `http://minio:9000` | URL MinIO API endpoint (thay thế AWS S3 endpoint) |
| `storage.class` | `S3Storage` | Backend storage implementation cho S3-compatible |

#### Định Dạng File

| Field | Giá Trị | Giải Thích |
|---|---|---|
| `format.class` | `AvroFormat` | Ghi file dưới dạng **Avro** (giữ nguyên schema từ Kafka) |

> **Lưu ý**: Có thể chuyển sang `ParquetFormat` để tương thích tốt hơn với các query engine (Spark, Trino, Athena), nhưng cần cấu hình thêm.

#### Phân Vùng Thời Gian (Partitioning)

| Field | Giá Trị | Giải Thích |
|---|---|---|
| `partitioner.class` | `TimeBasedPartitioner` | Phân vùng file theo thời gian của event |
| `path.format` | `'year'=YYYY/'month'=MM/'day'=dd` | Format thư mục. Dấu nháy đơn bao ngoài text literal |
| `partition.duration.ms` | `86400000` | 86400000ms = **24 giờ** — tạo thư mục mới mỗi ngày |
| `timezone` | `UTC` | Múi giờ dùng để tính timestamp khi phân vùng |
| `locale` | `en-US` | Locale cho format ngày tháng |

**Kết quả**: File sẽ nằm ở `northwind-data-lake/topics/northwind.public.orders/year=2024/month=01/day=15/`

#### Flush & Rotation

| Field | Giá Trị | Giải Thích |
|---|---|---|
| `flush.size` | `50` | Flush khi tích lũy đủ **50 records** |
| `rotate.interval.ms` | `60000` | Hoặc flush mỗi **60 giây** (60000ms), tùy điều kiện nào đến trước |

> **Tradeoff**: `flush.size` nhỏ → nhiều file nhỏ (less efficient để query). `flush.size` lớn → ít file hơn nhưng delay cao hơn. Giá trị `50` phù hợp cho môi trường dev/test.

---

## 3. Vòng Đời Connector

```
[Deploy]  → POST /connectors  → [RUNNING]
                                    ↓ Lỗi xảy ra
                               [FAILED]
                                    ↓ Khắc phục xong
                          PUT /connectors/{name}/restart → [RUNNING]
                                    ↓ Không cần nữa
                          DELETE /connectors/{name}  → [DELETED]
```

### Các Lệnh Quản Lý Qua REST API

```bash
# Xem danh sách connectors
curl http://localhost:8083/connectors

# Kiểm tra trạng thái
curl http://localhost:8083/connectors/source-postgres-debezium/status | jq

# Restart connector bị FAILED
curl -X POST http://localhost:8083/connectors/source-postgres-debezium/restart

# Xem cấu hình hiện tại
curl http://localhost:8083/connectors/source-postgres-debezium/config | jq

# Xóa connector
curl -X DELETE http://localhost:8083/connectors/source-postgres-debezium

# Cập nhật cấu hình (ví dụ thêm bảng)
curl -X PUT http://localhost:8083/connectors/source-postgres-debezium/config \
  -H "Content-Type: application/json" \
  -d '{ "table.include.list": "public.orders,...,public.new_table", ... }'
```

---

## 4. Xử Lý Sự Cố Connector

| Triệu Chứng | Kiểm Tra | Giải Pháp |
|---|---|---|
| Status `FAILED` | `GET /connectors/{name}/status` → xem `trace` | Xem log: `docker logs kafka-connect` |
| Không có messages trên topic | Kiểm tra Debezium có đang chạy không | Restart connector, kiểm tra Postgres WAL |
| File không xuất hiện ở MinIO | S3 Sink có RUNNING không? | Kiểm tra credentials MinIO trong `.env` |
| Schema registry conflict | Xem log kafka-connect | Xóa schema cũ hoặc tắt schema compatibility |
