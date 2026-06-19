import argparse
import logging
import random
import time
from datetime import datetime, timezone

from config.settings import KAFKA_SERVER_LOGS_TOPIC
from src.utils.kafka_client import build_text_producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("server_log_producer")

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic server log lines into Kafka.")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between log lines.")
    parser.add_argument("--max-events", type=int, default=None, help="Stop after this many log lines.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    producer = build_text_producer()
    logger.info("Producing server logs to topic: %s", KAFKA_SERVER_LOGS_TOPIC)

    try:
        sent = 0
        while args.max_events is None or sent < args.max_events:
            log_line = generate_log_line()
            producer.send(KAFKA_SERVER_LOGS_TOPIC, log_line)
            producer.flush()
            logger.info("sent log line: %s", log_line)
            sent += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Stopping server log producer on user request.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
