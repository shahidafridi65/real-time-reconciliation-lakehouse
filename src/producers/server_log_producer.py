import random
import time
from datetime import datetime, timezone

from config.settings import KAFKA_SERVER_LOGS_TOPIC
from src.utils.kafka_client import build_text_producer

producer = build_text_producer()

METHODS = ["GET", "POST", "PUT"]
ENDPOINTS = ["/home", "/search", "/product", "/cart", "/checkout"]
STATUSES = [200, 200, 200, 201, 400, 404, 500, 504]
SERVICES = ["api-gateway", "checkout-service", "catalog-service", "payment-service"]


def generate_log_line():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ip = ".".join(str(random.randint(1, 255)) for _ in range(4))
    method = random.choice(METHODS)
    endpoint = random.choice(ENDPOINTS)
    status = random.choice(STATUSES)
    latency = random.randint(10, 3000)
    service = random.choice(SERVICES)

    return f"{ts} {ip} {service} {method} {endpoint} {status} {latency}ms"


if __name__ == "__main__":
    print(f"Producing server logs to topic: {KAFKA_SERVER_LOGS_TOPIC}")

    while True:
        log_line = generate_log_line()
        producer.send(KAFKA_SERVER_LOGS_TOPIC, log_line)
        producer.flush()
        print(log_line)
        time.sleep(1)