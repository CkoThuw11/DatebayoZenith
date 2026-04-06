# 🔌 Connectors — Cấu Hình Kafka Connect

Thư mục này chứa các file cấu hình JSON cho **Kafka Connect connectors** của pipeline.

---

## Danh Sách Connectors

| File | Connector | Hướng | Mô Tả |
|---|---|---|---|
| `debezium-postgres.json` | `source-postgres-debezium` | Postgres → Kafka | Đọc WAL từ PostgreSQL, stream CDC events vào Kafka |
| `s3-sink-minio-production.json` | `minio-s3-sink-connector` | Kafka → MinIO | Consume Kafka topics, ghi Avro files vào MinIO Data Lake |

---

## Cách Đăng Ký

Các connectors được đăng ký tự động thông qua script:

```bash
# Chạy từ thư mục gốc project
bash scripts/register-connectors.sh
```

Hoặc đăng ký thủ công qua Kafka Connect REST API:

```bash
# Đăng ký Debezium Source Connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-postgres.json

# Đăng ký S3 Sink Connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/s3-sink-minio-production.json
```

---

## Tài Liệu Chi Tiết

Xem giải thích đầy đủ từng field cấu hình tại: **[docs/connectors.md](../docs/connectors.md)**
