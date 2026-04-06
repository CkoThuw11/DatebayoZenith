# 🛠️ Scripts — Tiện Ích Quản Lý Pipeline

Thư mục này chứa các script tiện ích để khởi tạo và quản lý pipeline.

---

## Danh Sách Scripts

| Script | Mô Tả |
|---|---|
| `register-connectors.sh` | Đăng ký cả hai Kafka Connect connectors và tạo MinIO bucket |

---

## `register-connectors.sh`

### Mô Tả

Script này thực hiện tuần tự các bước sau:

1. **Đợi Kafka Connect sẵn sàng** — poll REST API `GET /connectors` mỗi 5 giây cho đến khi nhận được HTTP 200
2. **Tạo MinIO bucket** `northwind-data-lake` (bỏ qua nếu đã tồn tại)
3. **Đăng ký Debezium Source Connector** từ `connectors/debezium-postgres.json`
4. **Đăng ký S3 Sink Connector** từ `connectors/s3-sink-minio-production.json`
5. **In trạng thái** các connectors vừa đăng ký

### Yêu Cầu

- `docker-compose up -d` đã được chạy trước
- `curl` có trong PATH
- Chạy từ **thư mục gốc** của project (`DatebayoZenith/`)

### Cách Chạy

```bash
# Cách 1 — Dùng bash tường minh (khuyến nghị trên Windows với Git Bash)
bash scripts/register-connectors.sh

# Cách 2 — Chạy trực tiếp (Linux/macOS)
./scripts/register-connectors.sh
```

### Kết Quả Mong Đợi

```
Waiting for Kafka Connect to be ready...
 Kafka Connect is ready!

Creating MinIO bucket...

Registering Debezium PostgreSQL connector...
{"name":"source-postgres-debezium","config":{...},"tasks":[...],"type":"source"}

Registering S3 MinIO sink connector...
{"name":"minio-s3-sink-connector","config":{...},"tasks":[...],"type":"sink"}

=========================================
All Connectors Registered!
=========================================

Current Connectors:
["source-postgres-debezium","minio-s3-sink-connector"]

Debezium Connector Status:
{"name":"source-postgres-debezium","connector":{"state":"RUNNING",...},...}

S3 Sink Connector Status:
{"name":"minio-s3-sink-connector","connector":{"state":"RUNNING",...},...}

Pipeline ready! PostgreSQL changes will flow to MinIO
```

### Lưu Ý Quan Trọng

> ⚠️ **Phải chạy từ thư mục gốc project** — Script dùng đường dẫn tương đối `connectors/debezium-postgres.json`. Nếu chạy từ trong thư mục `scripts/`, lệnh `curl -d @connectors/...` sẽ thất bại.

> ℹ️ Script dùng `mc` (MinIO Client) bên trong container `minio` để tạo bucket. Đảm bảo container `minio` đang chạy trước khi thực thi script.
