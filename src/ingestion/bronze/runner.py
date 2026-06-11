import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bronze_runner")

consume_topic = None
fetch_table_rows = None
persist_rows = None
fetch_shipping_payload = None
persist_payload = None


def _load_ingestion_functions():
    global consume_topic, fetch_table_rows, persist_rows, fetch_shipping_payload, persist_payload

    if consume_topic is None or fetch_table_rows is None or persist_rows is None:
        from src.ingestion.bronze.kafka_ingest import consume_topic as kafka_consume_topic
        from src.ingestion.bronze.postgres_ingest import fetch_table_rows as postgres_fetch_table_rows
        from src.ingestion.bronze.postgres_ingest import persist_rows as postgres_persist_rows
        from src.ingestion.bronze.shipping_ingest import fetch_shipping_payload as shipping_fetch_payload
        from src.ingestion.bronze.shipping_ingest import persist_payload as shipping_persist_payload

        consume_topic = kafka_consume_topic
        fetch_table_rows = postgres_fetch_table_rows
        persist_rows = postgres_persist_rows
        fetch_shipping_payload = shipping_fetch_payload
        persist_payload = shipping_persist_payload

    return consume_topic, fetch_table_rows, persist_rows, fetch_shipping_payload, persist_payload


def parse_args():
    parser = argparse.ArgumentParser(description="Run the Bronze ingestion flow for real sources")
    parser.add_argument("--bucket", default=None, help="Optional S3 Bronze bucket")
    parser.add_argument("--max-messages", type=int, default=5, help="Max Kafka records to upload")
    return parser.parse_args()


def run_bronze_ingestion(*, bucket=None, max_messages=5):
    """Run the Bronze ingestion orchestration flow for Kafka, PostgreSQL, and shipping data."""
    kafka_consume_topic, postgres_fetch_table_rows, postgres_persist_rows, shipping_fetch_payload, shipping_persist_payload = _load_ingestion_functions()

    summary = {
        "bucket": bucket,
        "max_messages": max_messages,
        "kafka_topics": 0,
        "postgres_tables": 0,
        "shipping_payload": False,
        "results": [],
    }

    logger.info("Starting Bronze ingestion flow")

    for topic_name in ("clickstream_events", "server_logs"):
        result = kafka_consume_topic(topic_name, max_messages=max_messages, bucket_name=bucket)
        summary["results"].append({"source": "kafka", "topic": topic_name, "result": result})
        summary["kafka_topics"] += 1

    for table_name in ("users", "products", "orders"):
        rows = postgres_fetch_table_rows(table_name, limit=20)
        result = postgres_persist_rows(table_name, rows, output_dir="bronze/raw/postgres", bucket_name=bucket)
        summary["results"].append({"source": "postgres", "table": table_name, "result": result})
        summary["postgres_tables"] += 1

    shipping_payload = shipping_fetch_payload()
    shipping_result = shipping_persist_payload(shipping_payload, output_dir="bronze/raw/shipping", bucket_name=bucket)
    summary["results"].append({"source": "shipping", "result": shipping_result})
    summary["shipping_payload"] = True

    logger.info("Bronze ingestion flow completed")
    return summary


def main():
    args = parse_args()
    run_bronze_ingestion(bucket=args.bucket, max_messages=args.max_messages)


if __name__ == "__main__":
    main()
