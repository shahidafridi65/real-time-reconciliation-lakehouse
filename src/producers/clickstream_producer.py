import random
import time

from datetime import datetime , timezone
from faker import Faker

from config.settings import KAFKA_CLICKSTREAM_TOPIC
from src.utils.kafka_client import  build_json_producer

fake = Faker()
producer = build_json_producer()

EVENT_TYPES = ["home_view", "search", "product_view", "add_to_cart", "purchase"]
DEVICES = ["mobile", "desktop", "tablet"]
CATEGORIES = ["electronics", "fashion", "home", "beauty", "sports"]

def generate_event():
    event_type = random.choices(
        EVENT_TYPES,
        weights=[12,9,22,7,3]
    )[0]

    return {
         "event_id": fake.uuid4(),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "user_id": random.randint(1000, 1049),
        "session_id": fake.uuid4(),
        "event_type": event_type,
        "product_id": random.randint(10000, 10050),
        "category": random.choice(CATEGORIES),
        "device_type": random.choice(DEVICES),
        "country": fake.country_code(),
        "price": round(random.uniform(20, 2000), 2),
        "quantity": random.randint(1, 3) if event_type in ["add_to_cart", "purchase"] else 0
    }

if __name__ == "__main__":
    print(f"Producing clickstream events to topic: {KAFKA_CLICKSTREAM_TOPIC}")

    while True:
        event = generate_event()
        producer.send(KAFKA_CLICKSTREAM_TOPIC, event)
        producer.flush()
        print(event)
        time.sleep(1)