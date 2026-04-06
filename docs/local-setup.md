# 🚀 Hướng Dẫn Chạy Pipeline Trên Local

Tài liệu này hướng dẫn từng bước để khởi động và kiểm tra toàn bộ CDC pipeline trên máy tính cá nhân.

---

## Yêu Cầu Hệ Thống

| Công Cụ | Phiên Bản | Kiểm Tra |
|---|---|---|
| Docker | ≥ 24.0 | `docker --version` |
| Docker Compose | ≥ 2.0 | `docker compose version` |
| curl | Bất kỳ | `curl --version` |
| Bash | Bất kỳ | Git Bash (Windows) / Terminal (macOS/Linux) |
| RAM trống | ≥ 4 GB | Kafka + Connect chiếm ~2 GB |
| Disk trống | ≥ 3 GB | Images + data volumes |

---

## Bước 1 — Clone & Chuẩn Bị Cấu Hình

```bash
# Clone repository
git clone <repo-url>
cd DatebayoZenith

# Tạo file .env từ template
cp .env.example .env
```

**File `.env` mặc định** (có thể giữ nguyên khi chạy local):
```env
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=admin123
```

---

## Bước 2 — Khởi Động Hạ Tầng

```bash
docker-compose up -d
```

**Kết quả mong đợi**:
```
[+] Running 6/6
 ✔ Container kafka            Started
 ✔ Container schema-registry  Started
 ✔ Container kafka-connect    Started
 ✔ Container akhq             Started
 ✔ Container minio            Started
 ✔ Container postgres-db      Started
```

> ⏳ **Lần đầu chạy** mất 5–10 phút vì `kafka-connect` cần tải và cài đặt 2 connector plugins (Debezium + S3 Sink). Đây là bình thường.

---

## Bước 3 — Kiểm Tra Trạng Thái Container

```bash
docker-compose ps
```

**Kết quả mong đợi** — tất cả phải ở trạng thái `running`:
```
NAME              IMAGE                                    STATUS
kafka             confluentinc/cp-kafka:7.4.0             running
schema-registry   confluentinc/cp-schema-registry:7.4.0  running
kafka-connect     confluentinc/cp-kafka-connect:7.4.0     running
akhq              tchiotludo/akhq                          running
minio             minio/minio                              running
postgres-db       postgres:15                             running
```

**Kiểm tra Kafka Connect đã sẵn sàng**:
```bash
curl -s http://localhost:8083/connectors
# Kết quả: []  (mảng rỗng — chưa có connector nào được đăng ký)
```

Nếu chưa có response, Kafka Connect đang khởi động — **đợi thêm vài phút**.

---

## Bước 4 — Đăng Ký Connectors

```bash
# Chạy từ thư mục gốc của project
bash scripts/register-connectors.sh
```

**Kết quả mong đợi**:
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
```

**Kiểm tra trạng thái connectors**:
```bash
curl -s http://localhost:8083/connectors/source-postgres-debezium/status | python -m json.tool
```

**Kết quả mong đợi**:
```json
{
  "name": "source-postgres-debezium",
  "connector": { "state": "RUNNING", "worker_id": "kafka-connect:8083" },
  "tasks": [{ "id": 0, "state": "RUNNING", "worker_id": "kafka-connect:8083" }],
  "type": "source"
}
```

---

## Bước 5 — Kiểm Tra Pipeline Hoạt Động

### 5a. Tạo Dữ Liệu Thử Trong Postgres

```bash
# Kết nối vào Postgres
docker exec -it postgres-db psql -U postgres -d northwind

# Thêm một đơn hàng mới
INSERT INTO orders (order_id, customer_id, employee_id, order_date, required_date)
VALUES (99999, 'ALFKI', 1, CURRENT_DATE, CURRENT_DATE + 7);

# Cập nhật đơn hàng
UPDATE orders SET ship_city = 'Hanoi' WHERE order_id = 99999;

# Thoát
\q
```

### 5b. Kiểm Tra Event Trên Kafka (AKHQ)

1. Mở http://localhost:8080
2. Chọn **Topics** → `northwind.public.orders`
3. Chọn tab **Messages** — bạn sẽ thấy các events INSERT và UPDATE vừa tạo

### 5c. Kiểm Tra File Trong MinIO

1. Mở http://localhost:9001
2. Đăng nhập: `admin` / `admin123`
3. Vào bucket `northwind-data-lake`
4. Duyệt theo đường dẫn `topics/northwind.public.orders/year=.../month=.../day=.../`
5. Sẽ thấy file `.avro` sau khi connector flush (tối đa 60 giây hoặc 50 records)

---

## Tóm Tắt Các Endpoint Web UI

| Giao Diện | URL | Đăng Nhập |
|---|---|---|
| AKHQ (Kafka UI) | http://localhost:8080 | Không cần |
| MinIO Console | http://localhost:9001 | `admin` / `admin123` |
| Kafka Connect REST | http://localhost:8083 | Không cần |
| Schema Registry | http://localhost:8081 | Không cần |

---

## Dừng Pipeline

```bash
# Dừng tất cả container (giữ lại data volumes)
docker-compose down

# Dừng và xóa toàn bộ data (làm sạch hoàn toàn)
docker-compose down -v
```

---

## Xử Lý Sự Cố

### Kafka Connect liên tục restart

```bash
docker logs kafka-connect --tail 50
```

Nếu thấy `Downloading connector...` → đang tải plugin, đợi thêm 5 phút.

---

### Connector ở trạng thái FAILED

```bash
curl -s http://localhost:8083/connectors/source-postgres-debezium/status | python -m json.tool
# Xem trường "tasks[0].trace" để biết lý do lỗi

# Restart connector
curl -X POST http://localhost:8083/connectors/source-postgres-debezium/restart
```

---

### MinIO bucket không được tạo (Credentials sai)

```bash
# Kiểm tra .env
cat .env

# Xem log của script
docker exec minio mc alias set local http://localhost:9000 admin admin123
```

Đảm bảo `MINIO_ROOT_USER` và `MINIO_ROOT_PASSWORD` trong `.env` khớp với lệnh trên.

---

### Port đã bị chiếm

```powershell
# Windows — Tìm process đang dùng port 9092
netstat -ano | findstr :9092
taskkill /PID <PID> /F
```

```bash
# macOS/Linux
lsof -i :9092
kill -9 <PID>
```
