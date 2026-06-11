import argparse
import json
import logging
from pathlib import Path

import requests

from config.settings import AWS_S3_BUCKET_NAME
import src.ingestion.bronze.s3_uploader as s3_uploader
from src.ingestion.bronze.schemas import validate_records

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bronze_shipping_ingest")


def fetch_shipping_payload(url: str = "http://localhost:8000/shipments"):
    """Fetch shipment records from the mock shipping API for Bronze ingestion."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()
    logger.info("Fetched %d shipping records from %s", len(payload.get("shipments", [])), url)
    return payload


def persist_payload(payload, output_dir: str | Path = "bronze/raw/shipping", bucket_name: str | None = None):
    """Persist the shipping API payload into Bronze raw storage.

    If an S3 Bronze bucket is configured, upload the payload directly to the cloud
    landing zone. Otherwise, write the JSON locally for inspection.
    """
    shipment_records = payload.get("shipments", []) if isinstance(payload, dict) else payload
    validated_payload = payload
    if isinstance(shipment_records, list):
        validated_payload = dict(payload)
        validated_payload["shipments"] = validate_records("shipment_data", shipment_records)

    bucket = bucket_name or AWS_S3_BUCKET_NAME
    if bucket:
        s3_uri = s3_uploader.upload_records_to_s3(validated_payload, source_name="shipping_payload", bucket_name=bucket)
        logger.info("Uploaded shipping payload to %s", s3_uri)
        return s3_uri

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / "shipping_payload.json"
    file_path.write_text(json.dumps(validated_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("Wrote shipping payload to %s", file_path)
    return str(file_path)


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest mock shipping API payload into the Bronze/raw landing path")
    parser.add_argument("--url", default="http://localhost:8000/shipments", help="Shipping API URL")
    parser.add_argument("--output-dir", default="bronze/raw/shipping", help="Output folder for Bronze raw data")
    parser.add_argument("--bucket", default=None, help="Optional AWS S3 bucket override for real Bronze uploads")
    return parser.parse_args()


def main():
    args = parse_args()
    payload = fetch_shipping_payload(args.url)
    persist_payload(payload, output_dir=args.output_dir, bucket_name=args.bucket)


if __name__ == "__main__":
    main()
