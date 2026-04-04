# 📋 Data & Infrastructure Contracts

Tài liệu này định nghĩa **ranh giới và cam kết** giữa các team. Mọi thành viên khi làm việc với pipeline đều **phải tuân thủ** các quy ước sau.

> ⚠️ **Lưu ý**: Bất kỳ thay đổi nào vi phạm các contracts này đều có thể gây **đứt gãy pipeline** hoặc **mất dữ liệu** ở downstream.

---

## 1. Quy Ước Đặt Tên

### Kafka Topics

**Pattern**: `{topic.prefix}.{schema}.{table}`

| Thành Phần | Giá Trị Hiện Tại | Ghi Chú |
|---|---|---|
| `topic.prefix` | `northwind` | Tên server DB, cấu hình trong Debezium |
| `schema` | `public` | Schema PostgreSQL |
| `table` | tên bảng | Chữ thường, dùng dấu `_` |

**Danh sách topics hiện tại**:

| Topic | Bảng Nguồn |
|---|---|
| `northwind.public.orders` | `public.orders` |
| `northwind.public.order_details` | `public.order_details` |
| `northwind.public.products` | `public.products` |
| `northwind.public.customers` | `public.customers` |

---

### Kafka Connect Connectors

**Pattern**: `{type}-{system}-{name}`

| Type | System | Name | Tên Đầy Đủ |
|---|---|---|---|
| `source` | `postgres` | `debezium` | `source-postgres-debezium` |
| `sink` | `s3` | `minio-connector` | `minio-s3-sink-connector` |

---

### MinIO Buckets

| Bucket | Mục Đích |
|---|---|
| `northwind-data-lake` | Bucket chính chứa toàn bộ CDC data |

---

## 2. Cấu Trúc Payload — Avro & Schema Registry

### Nguyên Tắc Bắt Buộc

- ✅ **Tất cả** messages trên Kafka topics PHẢI được serialize bằng **Avro**
- ✅ **Key** phải chứa **primary key** của bảng nguồn
- ✅ **Value** phải giữ nguyên **Debezium envelope** đầy đủ

---

### Debezium Envelope — Cấu Trúc Value

Mỗi message value trên Kafka có cấu trúc chuẩn Debezium:

```json
{
  "before": { ... },
  "after":  { ... },
  "source": { ... },
  "op":     "c | u | d | r",
  "ts_ms":  1700000000000
}
```

| Trường | Kiểu | Ý Nghĩa |
|---|---|---|
| `before` | Object \| null | Snapshot dữ liệu **trước** khi thay đổi. `null` với INSERT |
| `after` | Object \| null | Snapshot dữ liệu **sau** khi thay đổi. `null` với DELETE |
| `source` | Object | Metadata nguồn: tên DB, tên bảng, LSN của WAL, timestamp |
| `op` | String | Loại thao tác: `c`=CREATE, `u`=UPDATE, `d`=DELETE, `r`=READ (snapshot) |
| `ts_ms` | Long | Timestamp (milliseconds) khi Debezium xử lý event |

---

### Ví Dụ Schema Avro — Bảng `orders`

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

**Value Schema** (`northwind.public.orders-value`) — rút gọn:
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

### Ví Dụ Message Thực Tế

**Sự kiện: UPDATE đơn hàng `order_id = 10248`**

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

## 3. Cấu Trúc MinIO S3 Data Lake

### Layout Thư Mục

```
northwind-data-lake/                          ← Bucket
└── topics/                                   ← Prefix mặc định của S3 Sink
    ├── northwind.public.orders/
    │   └── year=2024/
    │       └── month=01/
    │           └── day=15/
    │               ├── northwind.public.orders+0+0000000000.avro
    │               └── northwind.public.orders+0+0000000050.avro
    │
    ├── northwind.public.order_details/
    │   └── year=2024/
    │       └── month=01/
    │           └── day=15/
    │               └── northwind.public.order_details+0+0000000000.avro
    │
    ├── northwind.public.products/
    │   └── ...
    │
    └── northwind.public.customers/
        └── ...
```

### Quy Tắc Đặt Tên File

**Pattern**: `{topic}+{partition}+{offset}.avro`

| Phần | Ví Dụ | Giải Thích |
|---|---|---|
| `topic` | `northwind.public.orders` | Tên Kafka topic |
| `partition` | `0` | Số partition Kafka |
| `offset` | `0000000050` | Offset đầu tiên trong file (10 chữ số, zero-padded) |

---

## 4. Chính Sách Schema Evolution

| Loại Thay Đổi | Được Phép? | Ghi Chú |
|---|---|---|
| Thêm column mới (nullable) | ✅ Có | Phải có `default: null` trong Avro schema |
| Xóa column | ❌ Không | Breaking change — cần migration plan |
| Đổi tên column | ❌ Không | Breaking change — cần alias |
| Thay đổi kiểu dữ liệu | ❌ Không | Breaking change — cần version mới |
| Thêm bảng mới vào CDC | ✅ Có | Cập nhật `table.include.list` trong Debezium config |

---

## 5. Quy Trình Khi Thay Đổi Contract

Khi cần thay đổi bất kỳ contract nào:

1. **Thông báo** cho tất cả team liên quan ít nhất **1 tuần** trước
2. **Tạo nhánh** riêng cho migration
3. **Kiểm tra** backward compatibility của Avro schema trước khi apply
4. **Cập nhật** tài liệu này cùng lúc với code change
5. **Không merge** cho đến khi downstream team xác nhận sẵn sàng