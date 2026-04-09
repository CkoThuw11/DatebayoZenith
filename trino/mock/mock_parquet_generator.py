"""
mock_parquet_generator.py
─────────────────────────
Tạo mock Parquet files trên MinIO để test Trino query
trước khi Spark pipeline hoàn thiện.

Cài đặt:
    pip install boto3 pandas pyarrow faker

Chạy:
    python trino/mock/mock_parquet_generator.py
"""

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
from datetime import date, timedelta, datetime
import random
from faker import Faker

# ── Cấu hình ──────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "admin123")
BUCKET = "northwind-data-lake"

# Ngày mock (hôm nay)
TODAY = date.today()
TODAY_TS_MS = int(datetime.combine(TODAY, datetime.min.time()).timestamp()) * 1000  # cross-platform
YEAR = str(TODAY.year)
MONTH = str(TODAY.month).zfill(2)
DAY = str(TODAY.day).zfill(2)

fake = Faker()

# ── Kết nối MinIO ──────────────────────────────────────
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)


def upload_parquet(df: pd.DataFrame, s3_key: str):
    """Upload DataFrame as Parquet to MinIO."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=buf.getvalue())
    print(f"  ✅ Uploaded: s3://{BUCKET}/{s3_key}  ({len(df)} rows)")


def ensure_bucket():
    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"Bucket '{BUCKET}' already exists.")
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Bucket '{BUCKET}' created.")


# ── Generator functions ────────────────────────────────

def gen_orders(n=100) -> pd.DataFrame:
    cdc_ops = ["c", "u", "r"]
    rows = []
    for i in range(n):
        order_date = TODAY - timedelta(days=random.randint(0, 30))
        rows.append({
            "order_id":         10000 + i,
            "customer_id":      fake.bothify(text="?????").upper(),
            "employee_id":      random.randint(1, 9),
            "order_date":       str(order_date),
            "required_date":    str(order_date + timedelta(days=7)),
            "shipped_date":     str(order_date + timedelta(days=3)) if random.random() > 0.3 else None,
            "ship_via":         random.randint(1, 3),
            "freight":          round(random.uniform(5, 200), 2),
            "ship_name":        fake.company(),
            "ship_address":     fake.street_address(),
            "ship_city":        fake.city(),
            "ship_region":      fake.state(),
            "ship_postal_code": fake.postcode(),
            "ship_country":     fake.country(),
            "cdc_op":           random.choice(cdc_ops),
            "cdc_ts_ms":        TODAY_TS_MS + random.randint(0, 999),
            "year":             YEAR,
            "month":            MONTH,
            "day":              DAY,
        })
    return pd.DataFrame(rows)


def gen_order_details(n=200) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            "order_id":   random.randint(10000, 10099),
            "product_id": random.randint(1, 77),
            "unit_price": round(random.uniform(5, 200), 2),
            "quantity":   random.randint(1, 50),
            "discount":   round(random.choice([0, 0.05, 0.1, 0.15, 0.2, 0.25]), 2),
            "cdc_op":     "r",
            "cdc_ts_ms":  TODAY_TS_MS,
            "year":       YEAR,
            "month":      MONTH,
            "day":        DAY,
        })
    return pd.DataFrame(rows)


def gen_products(n=50) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            "product_id":        i + 1,
            "product_name":      fake.word().capitalize() + " " + fake.word().capitalize(),
            "supplier_id":       random.randint(1, 29),
            "category_id":       random.randint(1, 8),
            "quantity_per_unit": f"{random.randint(1, 24)} boxes",
            "unit_price":        round(random.uniform(5, 250), 2),
            "units_in_stock":    random.randint(0, 150),
            "units_on_order":    random.randint(0, 100),
            "reorder_level":     random.randint(0, 30),
            "discontinued":      random.randint(0, 1),
            "cdc_op":            "r",
            "cdc_ts_ms":         TODAY_TS_MS,
            "year":              YEAR,
            "month":             MONTH,
            "day":               DAY,
        })
    return pd.DataFrame(rows)


def gen_customers(n=30) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            "customer_id":   fake.bothify(text="?????").upper(),
            "company_name":  fake.company(),
            "contact_name":  fake.name(),
            "contact_title": fake.job(),
            "address":       fake.street_address(),
            "city":          fake.city(),
            "region":        fake.state(),
            "postal_code":   fake.postcode(),
            "country":       fake.country(),
            "phone":         fake.phone_number(),
            "fax":           fake.phone_number(),
            "cdc_op":        "r",
            "cdc_ts_ms":     TODAY_TS_MS,
            "year":          YEAR,
            "month":         MONTH,
            "day":           DAY,
        })
    return pd.DataFrame(rows)


# ── Main ───────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("  Mock Parquet Generator — DatebayoZenith")
    print(f"  MinIO: {MINIO_ENDPOINT}  |  Date: {TODAY}")
    print("=" * 55)

    ensure_bucket()
    print()

    partition = f"year={YEAR}/month={MONTH}/day={DAY}"
    base = "parquet"

    tables = {
        "orders":        gen_orders(100),
        "order_details": gen_order_details(200),
        "products":      gen_products(50),
        "customers":     gen_customers(30),
    }

    for table_name, df in tables.items():
        print(f"[{table_name}]")
        s3_key = f"{base}/{table_name}/{partition}/mock-data.parquet"
        upload_parquet(df, s3_key)

    print()
    print("=" * 55)
    print("  Mock data ready! Now run Trino queries:")
    print()
    print("  docker exec -it trino trino")
    print("  > SELECT * FROM hive.northwind.orders LIMIT 10;")
    print("=" * 55)