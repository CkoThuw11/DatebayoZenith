-- ================================================================
-- Trino Table Initialization — Northwind CDC Data Lake
-- ================================================================
-- Chạy sau khi Trino healthy:
--   docker exec -it trino trino --file /init/create_tables.sql
-- ================================================================

-- Schema maps to MinIO bucket prefix (parquet/)
CREATE SCHEMA IF NOT EXISTS hive.northwind
WITH (location = 's3a://northwind-data-lake/parquet/');

-- ────────────────────────────────────────────
-- TABLE: orders
-- ────────────────────────────────────────────
DROP TABLE hive.northwind.orders;

CREATE TABLE hive.northwind.orders (
    order_id        INTEGER,
    customer_id     VARCHAR,
    employee_id     INTEGER,
    order_date      VARCHAR,    -- đổi từ DATE → VARCHAR
    required_date   VARCHAR,    -- đổi từ DATE → VARCHAR
    shipped_date    VARCHAR,    -- đổi từ DATE → VARCHAR
    ship_via        INTEGER,
    freight         DOUBLE,
    ship_name       VARCHAR,
    ship_address    VARCHAR,
    ship_city       VARCHAR,
    ship_region     VARCHAR,
    ship_postal_code VARCHAR,
    ship_country    VARCHAR,
    cdc_op          VARCHAR,
    cdc_ts_ms       BIGINT,
    year            VARCHAR,
    month           VARCHAR,
    day             VARCHAR
)
WITH (
    external_location = 's3a://northwind-data-lake/parquet/orders/',
    format = 'PARQUET',
    partitioned_by = ARRAY['year', 'month', 'day']
);

-- ────────────────────────────────────────────
-- TABLE: order_details
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hive.northwind.order_details (
    order_id    INTEGER,
    product_id  INTEGER,
    unit_price  DOUBLE,
    -- FIX: SMALLINT → INTEGER (Python int → Parquet INT64)
    quantity    INTEGER,
    discount    DOUBLE,
    cdc_op      VARCHAR,
    cdc_ts_ms   BIGINT,
    year        VARCHAR,
    month       VARCHAR,
    day         VARCHAR
)
WITH (
    external_location = 's3a://northwind-data-lake/parquet/order_details/',
    format = 'PARQUET',
    partitioned_by = ARRAY['year', 'month', 'day']
);

-- ────────────────────────────────────────────
-- TABLE: products
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hive.northwind.products (
    product_id        INTEGER,
    product_name      VARCHAR,
    supplier_id       INTEGER,
    category_id       INTEGER,
    quantity_per_unit VARCHAR,
    unit_price        DOUBLE,
    -- FIX: SMALLINT → INTEGER
    units_in_stock    INTEGER,
    units_on_order    INTEGER,
    reorder_level     INTEGER,
    discontinued      INTEGER,
    cdc_op            VARCHAR,
    cdc_ts_ms         BIGINT,
    year              VARCHAR,
    month             VARCHAR,
    day               VARCHAR
)
WITH (
    external_location = 's3a://northwind-data-lake/parquet/products/',
    format = 'PARQUET',
    partitioned_by = ARRAY['year', 'month', 'day']
);

-- ────────────────────────────────────────────
-- TABLE: customers
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hive.northwind.customers (
    customer_id   VARCHAR,
    company_name  VARCHAR,
    contact_name  VARCHAR,
    contact_title VARCHAR,
    address       VARCHAR,
    city          VARCHAR,
    region        VARCHAR,
    postal_code   VARCHAR,
    country       VARCHAR,
    phone         VARCHAR,
    fax           VARCHAR,
    cdc_op        VARCHAR,
    cdc_ts_ms     BIGINT,
    year          VARCHAR,
    month         VARCHAR,
    day           VARCHAR
)
WITH (
    external_location = 's3a://northwind-data-lake/parquet/customers/',
    format = 'PARQUET',
    partitioned_by = ARRAY['year', 'month', 'day']
);

-- ────────────────────────────────────────────
-- Sync partitions sau mỗi lần Spark chạy xong
-- (Trino file metastore cần lệnh này để nhận ra folder mới)
-- ────────────────────────────────────────────
CALL hive.system.sync_partition_metadata('northwind', 'orders', 'FULL');
CALL hive.system.sync_partition_metadata('northwind', 'order_details', 'FULL');
CALL hive.system.sync_partition_metadata('northwind', 'products', 'FULL');
CALL hive.system.sync_partition_metadata('northwind', 'customers', 'FULL');

SELECT 'All 4 Northwind tables ready in hive.northwind' AS status;