"""
Mock Avro Producer: Gui du lieu gia lap toi Kafka topic 'test_topic'
Dung de xac nhan S3 Sink Connector ghi dung file .avro vao MinIO
truoc khi tich hop voi Debezium that.

Cach chay:
    pip install confluent-kafka[avro] requests
    python scripts/testing/mock-producer.py
"""

import sys
import io
import json
import time
import random
from datetime import datetime, timedelta
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Fix Windows console encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


# ============================================================
# Cau hinh ket noi (tu may local -> Docker containers)
# ============================================================
KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "test_topic"
NUM_RECORDS = 1100  # So luong ban ghi gui di (>= flush.size=1000 de trigger flush)


# ============================================================
# Avro Schema - gia lap cau truc bang orders cua Northwind
# ============================================================
ORDER_SCHEMA_STR = json.dumps({
    "type": "record",
    "name": "Order",
    "namespace": "northwind.public",
    "fields": [
        {"name": "order_id", "type": "int"},
        {"name": "customer_id", "type": "string"},
        {"name": "employee_id", "type": "int"},
        {"name": "order_date", "type": "string"},
        {"name": "ship_city", "type": "string"},
        {"name": "ship_country", "type": "string"},
        {"name": "freight", "type": "float"},
        {"name": "status", "type": "string"}
    ]
})

KEY_SCHEMA_STR = json.dumps({
    "type": "record",
    "name": "OrderKey",
    "namespace": "northwind.public",
    "fields": [
        {"name": "order_id", "type": "int"}
    ]
})


# ============================================================
# Du lieu mau
# ============================================================
CUSTOMERS = ["ALFKI", "BERGS", "CENTC", "DRACD", "EASTC", "FRANK", "GOURL", "HILAA"]
CITIES = ["Hanoi", "Ho Chi Minh", "Da Nang", "Berlin", "London", "Paris", "Tokyo", "New York"]
COUNTRIES = ["Vietnam", "Vietnam", "Vietnam", "Germany", "UK", "France", "Japan", "USA"]
STATUSES = ["pending", "shipped", "delivered", "cancelled"]


def generate_order(order_id: int) -> dict:
    """Tao mot don hang gia lap."""
    city_idx = random.randint(0, len(CITIES) - 1)
    days_ago = random.randint(0, 30)
    order_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")

    return {
        "order_id": order_id,
        "customer_id": random.choice(CUSTOMERS),
        "employee_id": random.randint(1, 9),
        "order_date": order_date,
        "ship_city": CITIES[city_idx],
        "ship_country": COUNTRIES[city_idx],
        "freight": round(random.uniform(5.0, 200.0), 2),
        "status": random.choice(STATUSES)
    }


def delivery_report(err, msg):
    """Callback khi message duoc gui (hoac loi)."""
    if err is not None:
        print(f"  [ERROR] Loi gui message: {err}")
    else:
        print(f"  [OK] Gui thanh cong -> partition={msg.partition()}, offset={msg.offset()}")


def main():
    print("=" * 60)
    print("[PRODUCER] Mock Avro Producer - Northwind Orders")
    print(f"   Kafka:           {KAFKA_BOOTSTRAP}")
    print(f"   Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f"   Topic:           {TOPIC}")
    print(f"   So ban ghi:      {NUM_RECORDS}")
    print("=" * 60)

    # --- Schema Registry Client ---
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # --- Avro Serializers ---
    key_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=KEY_SCHEMA_STR
    )
    value_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=ORDER_SCHEMA_STR
    )

    # --- Kafka Producer ---
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    print(f"\n[SENDING] Dang gui {NUM_RECORDS} ban ghi toi topic '{TOPIC}'...\n")

    for i in range(1, NUM_RECORDS + 1):
        order = generate_order(order_id=i)
        key = {"order_id": order["order_id"]}

        # Serialize key & value theo Avro
        serialized_key = key_serializer(
            key, SerializationContext(TOPIC, MessageField.KEY)
        )
        serialized_value = value_serializer(
            order, SerializationContext(TOPIC, MessageField.VALUE)
        )

        producer.produce(
            topic=TOPIC,
            key=serialized_key,
            value=serialized_value,
            on_delivery=delivery_report
        )

        # Flush moi 10 ban ghi de khong qua tai buffer
        if i % 10 == 0:
            producer.flush()
            print(f"  [PROGRESS] Da gui {i}/{NUM_RECORDS} ban ghi...")

    # Dam bao tat ca message duoc gui xong
    producer.flush()
    print(f"\n[DONE] Hoan tat! Da gui {NUM_RECORDS} ban ghi toi topic '{TOPIC}'.")
    print("   Bay gio hay kiem tra:")
    print("   - AKHQ:  http://localhost:8080")
    print("   - MinIO: http://localhost:9001")


if __name__ == "__main__":
    main()