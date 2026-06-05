import random
import time
from datetime import datetime , timezone

from sqlalchemy import text

from src.utils.db import engine

def create_new_order(conn):
    quantity = random.randint(1,4)
    unit_price = round(random.uniform(20, 1500), 2)
    total_amount = round(quantity * unit_price, 2)

    '''
        This query is used for the finding the biggesr order in table ,
        adding the new order -> suppose table last order is 5005 . 

        COALESCE(5005 , 5000) -> if order exists then take it order_id or fill the NULL value with 5000
        new_order_id => 50005 + 1 = 5006
    '''
    new_order_id = conn.execute(
        text("SELECT COALESCE( MAX(order_id) , 5000 ) + 1 FROM orders")
    ).scalar()

    conn.execute(text(""" 
        INSERT INTO orders(
            order_id , user_id , product_id , quantity, 
            total_amount , order_status , payment_status , order_placed_at
        ) 
        VALUES (
            :order_id , :user_id , :product_id , :quantity, 
            :total_amount , :order_status , :payment_status , :order_placed_at
        ) """), {
            "order_id" : new_order_id ,
            "user_id": random.randint(1000,1049),
            "product_id": random.randint(10000,10050), 
            "quantity": quantity, 
            "total_amount": total_amount,
            "order_status": "PLACED",
            "payment_status": "PENDING",
            "order_placed_at": datetime.utcnow()
        })
    
    print(f"[INSERT] order_id={new_order_id}")

def update_order_lifecycle(conn):
    '''
        ORDER BY RANDOM() -> makes table data random 
        LIMIT 1 -> takes the 1st row

        .mappings() -> this convert the result of table data into  dictionary-like format.
        {
            "order_id": 50002,
            "order_status": "PLACED",
            "payment_status": "PENDING"
        } 
    '''
    order = conn.execute(text("""
        SELECT order_id, order_status, payment_status
        FROM orders
        ORDER BY RANDOM()
        LIMIT 1
    """)).mappings().first()

    if not order:
        return
    
    new_payment_status = order["payment_status"]
    new_order_status = order["order_status"]

    if order["payment_status"] == "PENDING":
        new_payment_status = random.choice(["PAID", "FAILED"])

    if order["order_status"] == "PLACED" and new_payment_status == "PAID":
        new_order_status = random.choice(["SHIPPED", "CANCELLED"])

    elif order["order_status"] == "SHIPPED":
        new_order_status = "DELIVERED"

    conn.execute(text("""
        UPDATE orders
        SET payment_status = :payment_status,
            order_status = :order_status
        WHERE order_id = :order_id
    """), {
        "payment_status": new_payment_status,
        "order_status": new_order_status,
        "order_id": order["order_id"]
    })

    print(
        f"[UPDATE] order_id={order['order_id']} "
        f"{order['payment_status']}->{new_payment_status}, "
        f"{order['order_status']}->{new_order_status}"
    )

def update_product_price(conn):
    product_id = random.randint(10000, 10050)
    new_price = round(random.uniform(20, 2000), 2)

    conn.execute(text("""
        UPDATE products
        SET price = :price,
            updated_at = :updated_at
        WHERE product_id = :product_id
    """), {
        "price": new_price,
        "updated_at": datetime.now(timezone.utc),
        "product_id": product_id
    })

    print(f"[PRICE UPDATE] product_id={product_id}, price={new_price}")

def run_simulator(max_iterations=None):
    iteration = 0
    print("Starting dynamic PostgreSQL transaction simulator...")

    try:
        while True:
            if max_iterations is not None and iteration >= max_iterations:
                print(f"Reached max_iterations={max_iterations}. Stopping simulator.")
                break

            action = random.choices(
                ["insert_order", "update_order", "update_product"],
                weights=[55, 30, 15]
            )[0]

            with engine.begin() as conn:
                if action == "insert_order":
                    create_new_order(conn)
                elif action == "update_order":
                    update_order_lifecycle(conn)
                else:
                    update_product_price(conn)

            iteration += 1
            time.sleep(random.randint(2, 5))

    except KeyboardInterrupt:
        print("\nSimulator stopped manually by user.")

if __name__ == "__main__":
    run_simulator()