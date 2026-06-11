import argparse
import logging
import random
import time

from datetime import datetime, timezone

from faker import Faker

from config.settings import KAFKA_CLICKSTREAM_TOPIC
from src.utils.kafka_client import build_json_producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("clickstream_producer")

fake = Faker()

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

def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic clickstream events into Kafka")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between events")
    parser.add_argument("--max-events", type=int, default=None, help="Stop after this many events")
    return parser.parse_args()


def main():
    args = parse_args()
    producer = build_json_producer()

    logger.info("Producing clickstream events to topic: %s", KAFKA_CLICKSTREAM_TOPIC)

    try:
        sent = 0
        while args.max_events is None or sent < args.max_events:
            event = generate_event()
            producer.send(KAFKA_CLICKSTREAM_TOPIC, event)
            producer.flush()
            logger.info("sent event id=%s type=%s", event["event_id"], event["event_type"])
            sent += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Stopping clickstream producer on user request.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()