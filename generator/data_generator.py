import os
import time
import random
import logging
from datetime import datetime, timedelta
import psycopg2
from faker import Faker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker()

DB_HOST        = os.getenv('DB_HOST',        'postgres-db')
DB_PORT        = os.getenv('DB_PORT',        '5432')
DB_NAME        = os.getenv('DB_NAME',        'northwind')
DB_USER        = os.getenv('DB_USER',        'postgres')
DB_PASS        = os.getenv('DB_PASS',        'postgres')
SLEEP_INTERVAL = int(os.getenv('SLEEP_INTERVAL', '10'))


def get_connection():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT,
                dbname=DB_NAME, user=DB_USER, password=DB_PASS,
            )
            return conn
        except psycopg2.OperationalError as e:
            logger.error(f"Could not connect to database, retrying in 5 seconds... Error: {e}")
            time.sleep(5)


def generate_order(conn):
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1;")
        customer_id = cursor.fetchone()[0]

        cursor.execute("SELECT employee_id FROM employees ORDER BY RANDOM() LIMIT 1;")
        employee_id = cursor.fetchone()[0]

        cursor.execute("SELECT shipper_id FROM shippers ORDER BY RANDOM() LIMIT 1;")
        ship_via = cursor.fetchone()[0]

        cursor.execute("SELECT COALESCE(MAX(order_id), 10000) + 1 FROM orders;")
        order_id = cursor.fetchone()[0]

        order_date    = datetime.now().date()
        required_date = order_date + timedelta(days=random.randint(5, 30))
        shipped_date  = order_date + timedelta(days=random.randint(1, 4))
        freight       = round(random.uniform(5.0, 150.0), 2)

        cursor.execute("""
            INSERT INTO orders (
                order_id, customer_id, employee_id, order_date, required_date, shipped_date,
                ship_via, freight, ship_name, ship_address, ship_city, ship_region,
                ship_postal_code, ship_country
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            order_id, customer_id, employee_id, order_date, required_date, shipped_date,
            ship_via, freight,
            fake.company()[:40], fake.street_address()[:60], fake.city()[:15],
            fake.state_abbr()[:15], fake.postcode()[:10], fake.country()[:15],
        ))

        cursor.execute(
            "SELECT product_id, unit_price FROM products ORDER BY RANDOM() LIMIT %s;",
            (random.randint(1, 5),),
        )
        products = cursor.fetchall()

        for product_id, unit_price in products:
            quantity = random.randint(1, 20)
            discount = random.choice([0.0, 0.05, 0.1, 0.15, 0.2])
            cursor.execute("""
                INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount)
                VALUES (%s, %s, %s, %s, %s)
            """, (order_id, product_id, unit_price, quantity, discount))

        conn.commit()
        logger.info(f"Generated order {order_id} with {len(products)} line items.")
        cursor.close()

    except Exception as e:
        logger.error(f"Error generating order: {e}")
        conn.rollback()


def insert_product(conn):
    """Insert a new product row — produces a CDC 'c' event on the products topic."""
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT COALESCE(MAX(product_id), 100) + 1 FROM products;")
        product_id = cursor.fetchone()[0]

        cursor.execute("SELECT category_id FROM categories ORDER BY RANDOM() LIMIT 1;")
        category_id = cursor.fetchone()[0]

        cursor.execute("SELECT supplier_id FROM suppliers ORDER BY RANDOM() LIMIT 1;")
        supplier_id = cursor.fetchone()[0]

        unit_price        = round(random.uniform(2.0, 200.0), 2)
        units_in_stock    = random.randint(0, 150)
        units_on_order    = random.randint(0, 50)
        reorder_level     = random.randint(0, 30)
        discontinued      = 0

        cursor.execute("""
            INSERT INTO products (
                product_id, product_name, supplier_id, category_id,
                quantity_per_unit, unit_price, units_in_stock,
                units_on_order, reorder_level, discontinued
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            product_id,
            fake.bs().title()[:40],
            supplier_id,
            category_id,
            f"{random.randint(1, 24)} units",
            unit_price,
            units_in_stock,
            units_on_order,
            reorder_level,
            discontinued,
        ))

        conn.commit()
        logger.info(f"Inserted new product {product_id} (price={unit_price}).")
        cursor.close()

    except Exception as e:
        logger.error(f"Error inserting product: {e}")
        conn.rollback()


def update_product(conn):
    """Update unit_price and units_in_stock on a random existing product — produces a CDC 'u' event."""
    try:
        cursor = conn.cursor()

        cursor.execute("SELECT product_id FROM products ORDER BY RANDOM() LIMIT 1;")
        row = cursor.fetchone()
        if row is None:
            return
        product_id = row[0]

        new_price       = round(random.uniform(2.0, 200.0), 2)
        new_stock       = random.randint(0, 150)

        cursor.execute("""
            UPDATE products
            SET unit_price     = %s,
                units_in_stock = %s
            WHERE product_id   = %s
        """, (new_price, new_stock, product_id))

        conn.commit()
        logger.info(f"Updated product {product_id}: unit_price={new_price}, units_in_stock={new_stock}.")
        cursor.close()

    except Exception as e:
        logger.error(f"Error updating product: {e}")
        conn.rollback()


if __name__ == '__main__':
    logger.info("Initializing Data Generator...")
    time.sleep(8)
    conn = get_connection()
    logger.info("Connected to Database. Starting to generate data...")

    try:
        cycle = 0
        while True:
            generate_order(conn)

            if random.random() < 0.20:
                insert_product(conn)

            if random.random() < 0.40:
                update_product(conn)

            cycle += 1
            time.sleep(SLEEP_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Terminating Data Generator...")
    finally:
        conn.close()