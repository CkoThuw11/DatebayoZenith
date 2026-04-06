# 🌀 DatebayoZenith — CDC Northwind Pipeline

![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-7.4.0-231F20?logo=apachekafka&logoColor=white)
![Debezium](https://img.shields.io/badge/Debezium-2.4.2-FF0000?logo=apacheKafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-C72E49?logo=minio&logoColor=white)

> **CDC (Change Data Capture) Pipeline** hoàn chỉnh — tự động bắt mọi thay đổi dữ liệu từ PostgreSQL và stream realtime vào MinIO Data Lake thông qua Apache Kafka.

---

## 📐 Kiến Trúc Tổng Quan

![Kiến trúc CDC Pipeline: PostgreSQL → Debezium → Kafka → Schema Registry → S3 Sink → MinIO](docs/assets/architecture.png)

**Luồng dữ liệu**: `PostgreSQL` → `Debezium` → `Kafka` ↔ `Schema Registry` → `S3 Sink` → `MinIO`

---

## 📚 Tài Liệu

Tất cả tài liệu kỹ thuật nằm trong thư mục `docs/`:

| Tài Liệu | Mô Tả |
|---|---|
| [📐 Architecture](docs/architecture.md) | Kiến trúc hệ thống, luồng dữ liệu chi tiết, và quyết định thiết kế |
| [📋 Contracts](docs/contracts.md) | Quy ước đặt tên, Avro schema, S3 layout — cam kết giữa các team |
| [🔌 Connectors](docs/connectors.md) | Giải thích chi tiết từng field trong cấu hình Kafka Connect |
| [🚀 Local Setup](docs/local-setup.md) | Hướng dẫn từng bước để chạy pipeline trên máy local |

---

## 🗂️ Cấu Trúc Repository

```
DatebayoZenith/
│
├── 📄 README.md                      # Tài liệu tổng quan (file này)
├── 📄 docker-compose.yaml            # Định nghĩa toàn bộ hạ tầng local
├── 📄 .env.example                   # Template biến môi trường
├── 📄 northwind.sql                  # Dữ liệu mẫu Northwind (backup)
│
├── 📁 docs/                          # Tài liệu kiến trúc & hợp đồng dữ liệu
│   ├── architecture.md               # Thiết kế hệ thống end-to-end
│   ├── contracts.md                  # Data & Infrastructure contracts
│   ├── connectors.md                 # Hướng dẫn cấu hình connectors
│   └── local-setup.md                # Hướng dẫn chạy local
│
├── 📁 connectors/                    # Cấu hình Kafka Connect plugins
│   ├── debezium-postgres.json        # Source connector (CDC từ Postgres)
│   └── s3-sink-minio-production.json # Sink connector (ghi vào MinIO)
│
├── 📁 scripts/                       # Script tiện ích
│   └── register-connectors.sh        # Đăng ký connectors qua REST API
│
└── 📁 postgres/                      # Khởi tạo database nguồn
    ├── init.sql                      # Schema Northwind + seed data
    └── README.md                     # Hướng dẫn cấu hình Postgres CDC
```

---

## ⚡ Bắt Đầu Nhanh

### Yêu Cầu Hệ Thống

| Công Cụ | Phiên Bản Tối Thiểu |
|---|---|
| Docker | 24.x trở lên |
| Docker Compose | 2.x trở lên |
| curl | Bất kỳ (dùng để đăng ký connectors) |
| Bash | Git Bash / WSL / Linux / macOS |

### Các Bước Khởi Động

**Bước 1 — Chuẩn bị cấu hình môi trường**
```bash
cp .env.example .env
# Chỉnh sửa .env nếu cần thay đổi credentials
```

**Bước 2 — Khởi động toàn bộ hạ tầng**
```bash
docker-compose up -d
```
> ⏳ Lần đầu chạy sẽ mất 5–10 phút để tải image và cài đặt connector plugins.

**Bước 3 — Kiểm tra tất cả container đang chạy**
```bash
docker-compose ps
```
Kết quả mong đợi: tất cả services ở trạng thái `Up`.

**Bước 4 — Đăng ký Kafka Connect Connectors**
```bash
# Chạy từ thư mục gốc của project
bash scripts/register-connectors.sh
```

**Bước 5 — Truy cập Web UI để kiểm tra**

| Giao Diện | URL | Mục Đích |
|---|---|---|
| **AKHQ** (Kafka UI) | http://localhost:8080 | Xem topics, messages, consumer groups |
| **MinIO** (S3 UI) | http://localhost:9001 | Duyệt files trong data lake |
| **Kafka Connect REST** | http://localhost:8083 | Quản lý connectors qua API |
| **Schema Registry** | http://localhost:8081 | Xem Avro schemas đã đăng ký |

> Xem hướng dẫn chi tiết hơn tại [docs/local-setup.md](docs/local-setup.md).

---

## 🔧 Xử Lý Sự Cố Thường Gặp

| Triệu Chứng | Nguyên Nhân | Giải Pháp |
|---|---|---|
| `kafka-connect` restart liên tục | Đang tải connector plugins | Đợi 3–5 phút, plugins cần download |
| Script báo connection refused | Kafka Connect chưa sẵn sàng | Script tự động đợi — kiên nhẫn chờ |
| MinIO bucket không tạo được | Credentials không khớp | Kiểm tra `MINIO_ROOT_USER/PASSWORD` trong `.env` |
| Connector ở trạng thái `FAILED` | Postgres chưa enable logical replication | Kiểm tra `wal_level=logical` trong docker-compose |

---

## 📖 Đọc Thêm

- Kiến trúc chi tiết → [docs/architecture.md](docs/architecture.md)
- Quy ước và data contracts → [docs/contracts.md](docs/contracts.md)
- Cấu hình connectors → [docs/connectors.md](docs/connectors.md)
