import argparse
import json
from typing import Any

import requests
from kafka import KafkaConsumer
from sqlalchemy import text

from config.settings import (
    AWS_S3_BUCKET_NAME,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CLICKSTREAM_TOPIC,
    KAFKA_SERVER_LOGS_TOPIC,
    MOCK_SHIPPING_API_HOST,
    MOCK_SHIPPING_API_PORT,
)
from src.ingestion.bronze.iceberg import append_records_to_iceberg, get_spark_session
from src.ingestion.bronze.postgres_ingest import persist_rows
from src.ingestion.bronze.s3_uploader import upload_records_to_s3
from src.ingestion.bronze.schemas import validate_records, validate_tabular_records
from src.ingestion.bronze.shipping_ingest import persist_payload
from src.utils.db import engine


POSTGRES_TABLES = ("users", "products", "orders")
KAFKA_TOPICS = (KAFKA_CLICKSTREAM_TOPIC, KAFKA_SERVER_LOGS_TOPIC)
ICEBERG_TABLE_MAP = {
    "clickstream_events": "clickstream",
    "server_logs": "server_logs",
    "orders": "order_changes",
    "users": "users",
    "products": "products",
    "shipping_payload": "shipment_data",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one Bronze ingestion batch.")
    parser.add_argument("--bucket", default=None, help="Override the configured S3 bucket.")
    parser.add_argument("--max-messages", type=int, default=5, help="Kafka messages to read per topic.")
    parser.add_argument("--postgres-limit", type=int, default=1000, help="Rows to read per PostgreSQL table.")
    parser.add_argument(
        "--write-iceberg",
        action="store_true",
        help="Append batch records to Glue Catalog Iceberg Bronze tables in addition to raw S3 JSON.",
    )
    return parser.parse_args()


def consume_topic(topic_name: str, max_messages: int, bucket_name: str | None = None) -> str:
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda value: value.decode("utf-8"),
    )

    records: list[dict[str, Any] | str] = []
    for message in consumer:
        value = message.value
        try:
            records.append(json.loads(value))
        except json.JSONDecodeError:
            records.append(value)

        if len(records) >= max_messages:
            break

    consumer.close()

    validated = validate_records(topic_name, records)
    return upload_records_to_s3(validated, topic_name, bucket_name)


def fetch_table_rows(table_name: str, limit: int) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT :limit"), {"limit": limit})
        return [dict(row._mapping) for row in result]


def fetch_shipping_payload() -> dict[str, Any]:
    host = "localhost" if MOCK_SHIPPING_API_HOST in {"0.0.0.0", "::"} else MOCK_SHIPPING_API_HOST
    api_url = f"http://{host}:{MOCK_SHIPPING_API_PORT}/shipments"
    response = requests.get(api_url, params={"limit": 50}, timeout=30)
    response.raise_for_status()
    return response.json()


def _append_to_iceberg_if_enabled(
    records: list[dict[str, Any]],
    source_name: str,
    *,
    enabled: bool,
    spark=None,
) -> str | None:
    if not enabled:
        return None
    table_name = ICEBERG_TABLE_MAP[source_name]
    return append_records_to_iceberg(records, table_name, spark=spark)


def run_bronze_ingestion(
    bucket: str | None = None,
    max_messages: int = 5,
    postgres_limit: int = 1000,
    write_iceberg: bool = False,
) -> dict[str, Any]:
    outputs: dict[str, Any] = {
        "kafka_topics": 0,
        "postgres_tables": 0,
        "shipping_payloads": 0,
        "s3_paths": [],
        "iceberg_tables": [],
    }

    if write_iceberg and not AWS_S3_BUCKET_NAME:
        raise ValueError("AWS_S3_BUCKET_NAME is required when --write-iceberg is enabled.")

    spark = get_spark_session("Bronze_Batch_Ingestion") if write_iceberg else None
    try:
        for topic_name in KAFKA_TOPICS:
            consumer_records: list[dict[str, Any] | str] = []
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=5000,
                value_deserializer=lambda value: value.decode("utf-8"),
            )
            for message in consumer:
                value = message.value
                try:
                    consumer_records.append(json.loads(value))
                except json.JSONDecodeError:
                    consumer_records.append(value)
                if len(consumer_records) >= max_messages:
                    break
            consumer.close()

            records = validate_records(topic_name, consumer_records)
            outputs["s3_paths"].append(upload_records_to_s3(records, topic_name, bucket))
            iceberg_table = _append_to_iceberg_if_enabled(records, topic_name, enabled=write_iceberg, spark=spark)
            if iceberg_table:
                outputs["iceberg_tables"].append(iceberg_table)
            outputs["kafka_topics"] += 1

        for table_name in POSTGRES_TABLES:
            rows = validate_tabular_records(table_name, fetch_table_rows(table_name, postgres_limit))
            outputs["s3_paths"].append(persist_rows(table_name, rows, bucket))
            iceberg_table = _append_to_iceberg_if_enabled(rows, table_name, enabled=write_iceberg, spark=spark)
            if iceberg_table:
                outputs["iceberg_tables"].append(iceberg_table)
            outputs["postgres_tables"] += 1

        payload = fetch_shipping_payload()
        shipments = validate_records("shipment_data", payload.get("shipments", []))
        outputs["s3_paths"].append(persist_payload({"shipments": shipments}, bucket))
        iceberg_table = _append_to_iceberg_if_enabled(shipments, "shipping_payload", enabled=write_iceberg, spark=spark)
        if iceberg_table:
            outputs["iceberg_tables"].append(iceberg_table)
        outputs["shipping_payloads"] += 1
    finally:
        if spark is not None:
            spark.stop()

    return outputs


def run_bronze_ingestion_legacy(bucket: str | None = None, max_messages: int = 5, postgres_limit: int = 1000) -> dict[str, Any]:
    """Compatibility wrapper for older callers."""
    return run_bronze_ingestion(bucket=bucket, max_messages=max_messages, postgres_limit=postgres_limit)


def _deprecated_run_bronze_ingestion(bucket: str | None = None, max_messages: int = 5, postgres_limit: int = 1000) -> dict[str, Any]:
    outputs: dict[str, Any] = {
        "kafka_topics": 0,
        "postgres_tables": 0,
        "shipping_payloads": 0,
        "s3_paths": [],
    }

    for topic_name in KAFKA_TOPICS:
        outputs["s3_paths"].append(consume_topic(topic_name, max_messages, bucket))
        outputs["kafka_topics"] += 1

    for table_name in POSTGRES_TABLES:
        rows = fetch_table_rows(table_name, postgres_limit)
        outputs["s3_paths"].append(persist_rows(table_name, rows, bucket))
        outputs["postgres_tables"] += 1

    payload = fetch_shipping_payload()
    outputs["s3_paths"].append(persist_payload(payload, bucket))
    outputs["shipping_payloads"] += 1

    return outputs


def main() -> None:
    args = parse_args()
    summary = run_bronze_ingestion(
        bucket=args.bucket,
        max_messages=args.max_messages,
        postgres_limit=args.postgres_limit,
        write_iceberg=args.write_iceberg,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
