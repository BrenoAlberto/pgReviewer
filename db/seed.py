import logging
import random
from datetime import datetime, timedelta
from io import StringIO

import psycopg2

from pgreviewer.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
NUM_USERS = 100_000
NUM_ORDERS = 500_000
NUM_PRODUCTS = 10_000
NUM_ORDER_ITEMS = 1_000_000
BATCH_SIZE = 10_000


def get_connection():
    return psycopg2.connect(str(settings.DATABASE_URL))


def clean_db(cur):
    logger.info("Cleaning up existing tables...")
    cur.execute("DROP TABLE IF EXISTS order_items CASCADE;")
    cur.execute("DROP TABLE IF EXISTS orders CASCADE;")
    cur.execute("DROP TABLE IF EXISTS products CASCADE;")
    cur.execute("DROP TABLE IF EXISTS users CASCADE;")


def create_schema(cur):
    logger.info("Creating schema...")
    cur.execute("""
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            username TEXT,
            last_login TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT,
            price DECIMAL(10, 2) NOT NULL,
            stock_quantity INTEGER DEFAULT 0
        );

        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            status TEXT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL,
            total_amount DECIMAL(12, 2) DEFAULT 0
        );

        CREATE TABLE order_items (
            id SERIAL PRIMARY KEY,
            order_id INTEGER REFERENCES orders(id),
            product_id INTEGER REFERENCES products(id),
            quantity INTEGER NOT NULL,
            price_at_purchase DECIMAL(10, 2) NOT NULL
        );
    """)


def seed_users(cur):
    logger.info(f"Seeding {NUM_USERS} users...")
    f = StringIO()
    for i in range(1, NUM_USERS + 1):
        username = f"user_{i}"
        email = f"user_{i}@example.com"
        # 15% NULL for last_login
        last_login = (
            None
            if random.random() < 0.15
            else (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat()
        )
        created_at = (
            datetime.now() - timedelta(days=random.randint(365, 365 * 3))
        ).isoformat()
        f.write(
            f"{email}\t{username}\t"
            f"{last_login if last_login else '\\N'}\t{created_at}\n"
        )

    f.seek(0)
    cur.copy_from(f, "users", columns=("email", "username", "last_login", "created_at"))


def seed_products(cur):
    logger.info(f"Seeding {NUM_PRODUCTS} products...")
    categories = [
        "Electronics",
        "Books",
        "Clothing",
        "Home",
        "Garden",
        "Toys",
        "Sports",
    ]
    f = StringIO()
    for i in range(1, NUM_PRODUCTS + 1):
        name = f"Product {i}"
        category = random.choice(categories) if random.random() > 0.15 else None
        price = round(random.uniform(5.0, 500.0), 2)
        stock = random.randint(0, 1000)
        f.write(f"{name}\t{category if category else '\\N'}\t{price}\t{stock}\n")

    f.seek(0)
    cur.copy_from(
        f, "products", columns=("name", "category", "price", "stock_quantity")
    )


def seed_orders(cur):
    logger.info(f"Seeding {NUM_ORDERS} orders with power-law distribution...")

    # Power-law distribution for user IDs
    # Pareto distribution to pick user_ids [1, NUM_USERS]
    # np.random.pareto(alpha) + 1 gives values starts at 1.0
    # We want to scale it to our user range.
    # A simpler way is to use a distribution that favors lower IDs.
    user_ids = [
        (int(random.paretovariate(1.2)) % NUM_USERS) + 1 for _ in range(NUM_ORDERS)
    ]

    statuses = ["completed", "shipped", "processing", "cancelled", "pending"]
    start_date = datetime.now() - timedelta(days=3 * 365)
    f = StringIO()

    for i in range(NUM_ORDERS):
        user_id = int(user_ids[i])
        status = random.choice(statuses)
        # Random time in the last 3 years
        created_at = (
            start_date + timedelta(seconds=random.randint(0, 3 * 365 * 24 * 3600))
        ).isoformat()
        f.write(f"{user_id}\t{status}\t{created_at}\n")

        if (i + 1) % BATCH_SIZE == 0:
            f.seek(0)
            cur.copy_from(f, "orders", columns=("user_id", "status", "created_at"))
            f = StringIO()

    if f.tell() > 0:
        f.seek(0)
        cur.copy_from(f, "orders", columns=("user_id", "status", "created_at"))


def seed_order_items(cur):
    logger.info(f"Seeding {NUM_ORDER_ITEMS} order items...")
    f = StringIO()
    for i in range(NUM_ORDER_ITEMS):
        order_id = random.randint(1, NUM_ORDERS)
        product_id = random.randint(1, NUM_PRODUCTS)
        quantity = random.randint(1, 5)
        # Price at purchase (can be different from current product price,
        # but we'll just randomize)
        price = round(random.uniform(5.0, 500.0), 2)
        f.write(f"{order_id}\t{product_id}\t{quantity}\t{price}\n")

        if (i + 1) % BATCH_SIZE == 0:
            f.seek(0)
            cur.copy_from(
                f,
                "order_items",
                columns=("order_id", "product_id", "quantity", "price_at_purchase"),
            )
            f = StringIO()

    if f.tell() > 0:
        f.seek(0)
        cur.copy_from(
            f,
            "order_items",
            columns=("order_id", "product_id", "quantity", "price_at_purchase"),
        )


def run_seed():
    conn = get_connection()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            clean_db(cur)
            create_schema(cur)
            conn.commit()

            seed_users(cur)
            conn.commit()

            seed_products(cur)
            conn.commit()

            seed_orders(cur)
            conn.commit()

            seed_order_items(cur)
            conn.commit()

            logger.info("Running ANALYZE...")
            cur.execute("ANALYZE;")
            conn.commit()

            logger.info("Done! Database successfully seeded.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Seeding failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run_seed()
