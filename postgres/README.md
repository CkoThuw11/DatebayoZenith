# 🗄️ Postgres — Database Nguồn CDC

**Owner**: Source Team

---

## Tổng Quan

Thư mục này chứa script khởi tạo cho **database nguồn** của pipeline. PostgreSQL được cấu hình với **Logical Replication** để Debezium có thể đọc WAL và capture mọi thay đổi dữ liệu theo thời gian thực.

Database sử dụng schema **Northwind** — một bộ dữ liệu thương mại mô phỏng kinh điển.

---

## Yêu Cầu Để Hỗ Trợ CDC

Postgres instance phải được cấu hình với các tham số sau (đã được set trong `docker-compose.yaml`):

| Tham Số | Giá Trị Yêu Cầu | Lý Do |
|---|---|---|
| `wal_level` | `logical` | Bật Logical Decoding — cần thiết để Debezium đọc WAL |
| `max_replication_slots` | ≥ 1 | Số slot nhân bản (mỗi Debezium connector dùng 1 slot) |
| `max_wal_senders` | ≥ 1 | Số kết nối WAL sender (mỗi slot cần 1 sender) |
| User permission | `REPLICATION` | User kết nối phải có quyền `REPLICATION` |

---

## Các Bảng Được CDC Theo Dõi

Debezium hiện được cấu hình để theo dõi **4 bảng** trong schema `public`:

| Bảng | Mô Tả | Kafka Topic |
|---|---|---|
| `public.orders` | Đơn hàng của khách | `northwind.public.orders` |
| `public.order_details` | Chi tiết sản phẩm trong đơn | `northwind.public.order_details` |
| `public.products` | Danh mục sản phẩm | `northwind.public.products` |
| `public.customers` | Thông tin khách hàng | `northwind.public.customers` |

### Cột Chính Quan Trọng

**`orders`**:
- `order_id` (PK) — Được dùng làm Avro **key**
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

## Cơ Chế Replica Identity

Mặc định PostgreSQL chỉ ghi giá trị `after` vào WAL khi UPDATE. Với cấu hình:

```sql
ALTER TABLE public.orders REPLICA IDENTITY FULL;
```

Postgres sẽ ghi **cả `before` và `after`** — cho phép downstream biết được dữ liệu **trước khi thay đổi**.

Debezium tự động thực hiện điều này thông qua tham số:
```json
"replica.identity.autoset.values": "public.*:FULL"
```

---

## Khởi Tạo Local

Khi chạy `docker-compose up`, file `init.sql` sẽ được tự động thực thi để tạo toàn bộ schema Northwind và seed dữ liệu mẫu:

```bash
# Khởi tạo database (tự động khi docker-compose up)
docker-compose up -d postgres-db

# Kiểm tra database đã sẵn sàng
docker exec postgres-db psql -U postgres -d northwind -c "\dt public.*"
```

**Kết quả mong đợi** — danh sách các bảng Northwind:
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

## Kết Nối Tới Postgres

```bash
# Dùng psql trong container
docker exec -it postgres-db psql -U postgres -d northwind

# Dùng client ngoài (DBeaver, TablePlus, ...)
Host:     localhost
Port:     5432
Database: northwind
User:     postgres
Password: postgres
```
