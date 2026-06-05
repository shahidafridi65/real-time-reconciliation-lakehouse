import random
from datetime import datetime, timedelta, timezone

from faker import Faker
from sqlalchemy import text

from src.utils.db import engine

fake = Faker()
DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        full_name TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        country TEXT,
        created_at TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS products (
        product_id INT PRIMARY KEY,
        product_name TEXT NOT NULL,
        category TEXT NOT NULL,
        price NUMERIC(10,2) NOT NULL,
        updated_at TIMESTAMP NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id INT PRIMARY KEY,
        user_id INT NOT NULL,
        product_id INT NOT NULL,
        quantity INT NOT NULL,
        total_amount NUMERIC(10,2) NOT NULL,
        order_status TEXT NOT NULL,
        payment_status TEXT NOT NULL,
        order_placed_at TIMESTAMP NOT NULL
    )
    """
]

def seed_users(conn):
    for user_id in range(1000,1050):
        conn.execute(text("""
            INSERT INTO users (user_id, full_name, email, country, created_at)
            VALUES (:user_id, :full_name, :email, :country, :created_at)
            ON CONFLICT (user_id) DO NOTHING
        """), {
            "user_id": user_id,
            "full_name": fake.name(),
            "email": fake.unique.email(),
            "country": fake.country_code(),
            "created_at": fake.date_time_between(start_date="-2y", end_date="now")
        })

def seed_products(conn):
    categories = ["electronics", "fashion", "home", "beauty", "sports"]

    for product_id in range(10000, 10051):
        conn.execute(text("""
            INSERT INTO products (product_id, product_name, category, price, updated_at)
            VALUES (:product_id, :product_name, :category, :price, :updated_at)
            ON CONFLICT (product_id) DO NOTHING
        """), {
            "product_id": product_id,
            "product_name": fake.word().title() + " Item",
            "category": random.choice(categories),
            "price": round(random.uniform(20, 2000), 2),
            "updated_at": fake.date_time_between(start_date="-6m", end_date="now")
        })

def seed_initial_orders(conn):
    for order_id in range(50000, 50021):
        quantity = random.randint(1, 4)
        unit_price = round(random.uniform(20, 2000), 2)

        conn.execute(text("""
            INSERT INTO orders (
                order_id, user_id, product_id, quantity,
                total_amount, order_status, payment_status, order_placed_at
            )
            VALUES (
                :order_id, :user_id, :product_id, :quantity,
                :total_amount, :order_status, :payment_status, :order_placed_at
            )
            ON CONFLICT (order_id) DO NOTHING
        """), {
            "order_id": order_id,
            "user_id": random.randint(1000, 1049),
            "product_id": random.randint(10000, 10050),
            "quantity": quantity,
            "total_amount": round(quantity * unit_price, 2),
            "order_status": random.choice(["PLACED", "SHIPPED", "DELIVERED"]),
            "payment_status": random.choice(["PENDING", "PAID"]),
            "order_placed_at": datetime.now(timezone.utc) - timedelta(days=random.randint(1, 30))
        })


if __name__ == "__main__":
    with engine.begin() as conn:
        for ddl in DDL_STATEMENTS:
            conn.execute(text(ddl))
        
        seed_users(conn)
        seed_products(conn)
        seed_initial_orders(conn)
        
    print("Seeded users, products, and starter orders.")