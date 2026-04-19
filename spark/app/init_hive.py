import os
import logging
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] init_hive - %(message)s",
)
logger = logging.getLogger("init_hive")

# ---------------------------------------------------------------------------
# Configuration (reuse from your pipeline)
# ---------------------------------------------------------------------------
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "admin123")
BUCKET           = os.getenv("S3_BUCKET", "northwind-data-lake")

TABLE_CONFIG = {
    "orders":        {"pk": ["order_id"]},
    "order_details": {"pk": ["order_id", "product_id"]},
    "products":      {"pk": ["product_id"]},
    "customers":     {"pk": ["customer_id"]},
}

DATABASE = "northwind"

# ---------------------------------------------------------------------------
# Spark Session (with Hive support)
# ---------------------------------------------------------------------------
def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("cdc-pipeline")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_sink_path(table_name: str) -> str:
    return f"s3a://{BUCKET}/{DATABASE}/{table_name}/"

def table_exists(spark: SparkSession, table_name: str) -> bool:
    return spark.catalog.tableExists(f"{DATABASE}.{table_name}")

# ---------------------------------------------------------------------------
# Schema & Table Creation
# ---------------------------------------------------------------------------
# DLL scripts
# ---------------------------------------------------------------------------
# Full DDL per table (SOURCE OF TRUTH)
# ---------------------------------------------------------------------------
TABLE_DDL = {
    "customers": f"""
        CREATE TABLE {DATABASE}.customers (
            customer_id STRING,
            company_name STRING,
            contact_name STRING,
            contact_title STRING,
            address STRING,
            city STRING,
            region STRING,
            postal_code STRING,
            country STRING,
            phone STRING,
            fax STRING,
            cdc_ts_ms BIGINT,
            cdc_op STRING
        )
        USING PARQUET
        PARTITIONED BY (year INT, month INT, day INT)
        LOCATION 's3a://{BUCKET}/{DATABASE}/customers/'
    """,

    "order_details": f"""
        CREATE TABLE {DATABASE}.order_details (
            order_id SMALLINT,
            product_id SMALLINT,
            unit_price FLOAT,
            quantity SMALLINT,
            discount FLOAT,
            cdc_ts_ms BIGINT,
            cdc_op STRING
        )
        USING PARQUET
        PARTITIONED BY (year INT, month INT, day INT)
        LOCATION 's3a://{BUCKET}/{DATABASE}/order_details/'
    """,

    "orders": f"""
        CREATE TABLE {DATABASE}.orders (
            order_id SMALLINT,
            customer_id STRING,
            employee_id SMALLINT,
            order_date DATE,
            required_date DATE,
            shipped_date DATE,
            ship_via SMALLINT,
            freight FLOAT,
            ship_name STRING,
            ship_address STRING,
            ship_city STRING,
            ship_region STRING,
            ship_postal_code STRING,
            ship_country STRING,
            cdc_ts_ms BIGINT,
            cdc_op STRING
        )
        USING PARQUET
        PARTITIONED BY (year INT, month INT, day INT)
        LOCATION 's3a://{BUCKET}/{DATABASE}/orders/'
    """,

    "products": f"""
        CREATE TABLE {DATABASE}.products (
            product_id SMALLINT,
            product_name STRING,
            supplier_id SMALLINT,
            category_id SMALLINT,
            quantity_per_unit STRING,
            unit_price FLOAT,
            units_in_stock SMALLINT,
            units_on_order SMALLINT,
            reorder_level SMALLINT,
            discontinued INT,
            cdc_ts_ms BIGINT,
            cdc_op STRING
        )
        USING PARQUET
        PARTITIONED BY (year INT, month INT, day INT)
        LOCATION 's3a://{BUCKET}/{DATABASE}/products/'
    """
}

def create_database(spark: SparkSession):
    logger.info("Creating database if not exists: %s", DATABASE)
    spark.sql(f"""
    CREATE DATABASE IF NOT EXISTS {DATABASE}
    LOCATION 's3a://{BUCKET}/{DATABASE}'
    """)

def create_table(spark: SparkSession, table_name: str):
    if table_exists(spark, table_name):
        logger.info("Table already exists, skipping: %s.%s", DATABASE, table_name)
        return

    logger.info("Creating table: %s.%s", DATABASE, table_name)

    ddl = TABLE_DDL[table_name]
    spark.sql(ddl)

    logger.info("Table created: %s.%s", DATABASE, table_name)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 60)
    logger.info("Initializing Hive Metastore (Schema + Tables)")
    logger.info("=" * 60)

    spark = build_spark_session()

    # Test Hive connection
    try:
        spark.sql("SHOW DATABASES").show(5)
        logger.info("Hive Metastore connection OK")
    except Exception as e:
        logger.error("Cannot connect to Hive Metastore: %s", e)
        raise

    # Create database
    create_database(spark)

    # Create tables
    for table_name in TABLE_CONFIG.keys():
        try:
            create_table(spark, table_name)
        except Exception as e:
            logger.error("Failed to create table %s: %s", table_name, e, exc_info=True)

    spark.stop()
    logger.info("Hive initialization completed successfully")

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()