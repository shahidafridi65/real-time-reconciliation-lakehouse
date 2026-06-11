import argparse
import json
import logging

from kafka import KafkaConsumer

from config.settings import KAFKA_BOOTSTRAP_SERVERS
from src.ingestion.bronze.schemas import validate_records
from src.ingestion.bronze.s3_uploader import upload_records_to_s3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bronze_ingest")


def decode_value(value):
    if isinstance(value, (bytes, bytearray)):
        text = value.decode("utf-8")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

    return value


def consume_topic(topic_name: str, max_messages: int | None = None, bucket_name: str | None = None):
    """Consume a Kafka topic and upload the raw records to real AWS S3 Bronze storage."""
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: decode_value(value),
    )

    records = []
    try:
        for message in consumer:
            raw_value = decode_value(message.value)
            records.append(raw_value)

            if max_messages is not None and len(records) >= max_messages:
                break
    finally:
        consumer.close()

    validated_records = validate_records(topic_name, records)

    s3_uri = upload_records_to_s3(validated_records, source_name=topic_name, bucket_name=bucket_name)
    logger.info("Uploaded %d raw messages from topic %s to %s", len(validated_records), topic_name, s3_uri)
    return s3_uri


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest Kafka topics into real AWS S3 Bronze storage")
    parser.add_argument("--topic", required=True, help="Kafka topic to ingest")
    parser.add_argument("--max-messages", type=int, default=None, help="Stop after this many messages")
    parser.add_argument("--bucket", default=None, help="Optional AWS S3 bucket override")
    return parser.parse_args()


def main():
    args = parse_args()
    consume_topic(args.topic, max_messages=args.max_messages, bucket_name=args.bucket)


if __name__ == "__main__":
    main()
