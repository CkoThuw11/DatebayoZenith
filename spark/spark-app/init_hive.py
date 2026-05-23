import os
import logging
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] init_hive - %(message)s",
)
logger = logging.getLogger("init_hive")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
RAW_BUCKET    = os.getenv("RAW_BUCKET", "raw")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")

DATABASE = "northwind"

TABLES = ["orders", "order_details", "products"]


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("init-hive")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("hive.metastore.uris", "thrift://hive-metastore:9083")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def table_exists(spark: SparkSession, table_name: str) -> bool:
    return spark.catalog.tableExists(f"{DATABASE}.{table_name}")


def parquet_path(table_name: str) -> str:
    return f"s3a://{BRONZE_BUCKET}/northwind/{table_name}/"


TABLE_DDL = {
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
        LOCATION '{parquet_path("order_details")}'
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
        LOCATION '{parquet_path("orders")}'
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
        LOCATION '{parquet_path("products")}'
    """
}


def create_database(spark: SparkSession):
    logger.info("Creating database if not exists: %s", DATABASE)
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {DATABASE}
        LOCATION 's3a://{BRONZE_BUCKET}/northwind'
    """)


def create_table(spark: SparkSession, table_name: str):
    if table_exists(spark, table_name):
        logger.info("Table already exists, skipping: %s.%s", DATABASE, table_name)
        return
    logger.info("Creating table: %s.%s", DATABASE, table_name)
    spark.sql(TABLE_DDL[table_name])
    logger.info("Table created: %s.%s", DATABASE, table_name)


def main():
    logger.info("Initializing Hive Metastore (Schema + Tables)")
    logger.info("Bronze bucket (Parquet/Trino): %s", BRONZE_BUCKET)
    logger.info("Raw bucket (Avro/S3 Sink):     %s", RAW_BUCKET)

    spark = build_spark_session()

    try:
        spark.sql("SHOW DATABASES").show(5)
        logger.info("Hive Metastore connection OK")
    except Exception as e:
        logger.error("Cannot connect to Hive Metastore: %s", e)
        raise

    create_database(spark)

    for table_name in TABLES:
        try:
            create_table(spark, table_name)
        except Exception as e:
            logger.error("Failed to create table %s: %s", table_name, e, exc_info=True)

    spark.stop()
    logger.info("Hive initialization completed successfully")


if __name__ == "__main__":
    main()