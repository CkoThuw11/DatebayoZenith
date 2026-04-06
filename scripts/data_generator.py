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

DB_HOST = os.getenv('DB_HOST', 'postgres-db')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'northwind')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'postgres')
SLEEP_INTERVAL = int(os.getenv('SLEEP_INTERVAL', '10'))

def get_connection():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            return conn
        except psycopg2.OperationalError as e:
            logger.error(f"Could not connect to database, retrying in 5 seconds... Error: {e}")
            time.sleep(5)

def generate_order(conn):
    try:
        cursor = conn.cursor()
        
        # Get random foreign keys
        cursor.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1;")
        customer_id = cursor.fetchone()[0]

        cursor.execute("SELECT employee_id FROM employees ORDER BY RANDOM() LIMIT 1;")
        employee_id = cursor.fetchone()[0]

        cursor.execute("SELECT shipper_id FROM shippers ORDER BY RANDOM() LIMIT 1;")
        ship_via = cursor.fetchone()[0]

        # Get next order_id
        cursor.execute("SELECT COALESCE(MAX(order_id), 10000) + 1 FROM orders;")
        order_id = cursor.fetchone()[0]

        # Generate order details
        order_date = datetime.now().date()
        required_date = order_date + timedelta(days=random.randint(5, 30))
        shipped_date = order_date + timedelta(days=random.randint(1, 4))
        freight = round(random.uniform(5.0, 150.0), 2)
        
        # Insert into orders
        cursor.execute("""
            INSERT INTO orders (
                order_id, customer_id, employee_id, order_date, required_date, shipped_date, 
                ship_via, freight, ship_name, ship_address, ship_city, ship_region, 
                ship_postal_code, ship_country
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            order_id, customer_id, employee_id, order_date, required_date, shipped_date,
            ship_via, freight, fake.company()[:40], fake.street_address()[:60], fake.city()[:15], fake.state_abbr()[:15],
            fake.postcode()[:10], fake.country()[:15]
        ))
        
        # Generate 1 to 5 order_details items
        cursor.execute("SELECT product_id, unit_price FROM products ORDER BY RANDOM() LIMIT %s;", (random.randint(1, 5),))
        products = cursor.fetchall()
        
        for product_id, unit_price in products:
            quantity = random.randint(1, 20)
            discount = random.choice([0.0, 0.05, 0.1, 0.15, 0.2])
            
            cursor.execute("""
                INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount)
                VALUES (%s, %s, %s, %s, %s)
            """, (order_id, product_id, unit_price, quantity, discount))

        conn.commit()
        logger.info(f"Successfully generated Order {order_id} with {len(products)} line items.")
                
        cursor.close()
    except Exception as e:
        logger.error(f"Error generating order: {e}")
        conn.rollback()

if __name__ == '__main__':
    logger.info("Initializing Data Generator...")
    time.sleep(10) # Initial sleep to allow Postgres to fully start on compose up
    conn = get_connection()
    logger.info("Connected to Database. Starting to generate orders...")
    
    try:
        while True:
            generate_order(conn)
            time.sleep(SLEEP_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Terminating Data Generator...")
    finally:
        conn.close()
