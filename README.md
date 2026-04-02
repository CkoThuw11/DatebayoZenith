# CDC Northwind Pipeline

## Overview
This is the root repository for the Northwind CDC (Change Data Capture) Pipeline. It acts as the structural foundation and contract between the Source, Backbone, and Sink teams.

## Structure
- `docs/`: Critical architecture and contract definitions.
- `docker-compose.yaml`: The single source of truth for local infrastructure (Backbone-owned).
- `connectors/`: Integration boundary for Source and Sink configurations.
- `scripts/`: Shared standalone utilities.

## Getting Started
1. Review `docs/architecture.md` and `docs/contracts.md`.
2. Copy `.env.example` to `.env` and provide your local configurations.
3. Bring up the infrastructure (Requires `docker-compose`).


# Source Team (M1) — Postgres + Debezium

**Owner:** Thành viên 1 | **Branch:** `feature/source-postgres`

---

## Đã làm gì

- Thêm service `postgres-db` vào `docker-compose.yaml` với `wal_level=logical`, `max_replication_slots=10`
- Tạo `postgres/init.sql` chứa toàn bộ schema Northwind (14 bảng) + sample data + `REPLICA IDENTITY FULL` cho 4 bảng CDC
- Deploy Debezium connector (`inventory-connector`) theo dõi 4 bảng: `orders`, `order_details`, `products`, `customers`
- Connector status: **RUNNING** ✅

---

## Thông tin quan trọng cho M2 và M3

| Thông tin | Giá trị |
|---|---|
| Topic pattern | `northwind.public.<tên_bảng>` |
| Key/Value format | **Avro** (AvroConverter) |
| Schema Registry | `http://schema-registry:8081` |
| 4 topic chính | `northwind.public.orders` (4p), `northwind.public.order_details` (4p), `northwind.public.products` (2p), `northwind.public.customers` (2p) |

---

## Lưu ý khi gặp sự cố

**Kafka không start** → Dùng `CLUSTER_ID` (không phải `KAFKA_CLUSTER_ID`) trong docker-compose

**Kafka Connect crash** → Phải có 3 dòng replication factor = 1:
```yaml
CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: "1"
CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: "1"
CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: "1"
```

**Debezium chưa cài** → `kafka-connect` cần thêm command cài plugin khi start:
```yaml
command:
  - bash
  - -c
  - |
    confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.4.2
    /etc/confluent/docker/run
```

**Postgres volume cũ** → Chạy `docker compose down -v` trước khi up lại để `init.sql` chạy lại từ đầu
