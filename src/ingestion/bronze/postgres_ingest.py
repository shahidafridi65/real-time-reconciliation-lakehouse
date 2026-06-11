import argparse
import json
import logging
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import text

from config.settings import AWS_S3_BUCKET_NAME
import src.ingestion.bronze.s3_uploader as s3_uploader
from src.utils.db import engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bronze_postgres_ingest")


def normalize_value(value):
    """Convert Python-native values into JSON-safe values for Bronze storage."""
    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, dict):
        return {key: normalize_value(item) for key, item in value.items()}

    if isinstance(value, (list, tuple)):
        return [normalize_value(item) for item in value]

    return value


def fetch_table_rows(table_name: str, limit: int = 100):
    """Fetch raw rows from PostgreSQL for Bronze ingestion."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT :limit"), {"limit": limit})
        rows = [dict(row) for row in result.mappings().all()]

    logger.info("Fetched %d rows from table %s", len(rows), table_name)
    return rows


def persist_rows(table_name: str, rows, output_dir: str | Path = "bronze/raw/postgres", bucket_name: str | None = None):
    """Persist PostgreSQL table rows into the Bronze raw landing area.

    When an S3 Bronze bucket is available, upload the raw JSON payload there for the
    real cloud-based ingestion path. Otherwise, fall back to a local file in the
    Bronze/raw/postgres folder.
    """
    normalized_rows = [normalize_value(row) for row in rows]

    bucket = bucket_name or AWS_S3_BUCKET_NAME
    if bucket:
        s3_uri = s3_uploader.upload_records_to_s3(normalized_rows, source_name=table_name, bucket_name=bucket)
        logger.info("Uploaded %d raw rows from table %s to %s", len(rows), table_name, s3_uri)
        return s3_uri

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / f"{table_name}.json"
    file_path.write_text(json.dumps(normalized_rows, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("Wrote %d raw rows to %s", len(normalized_rows), file_path)
    return str(file_path)


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest PostgreSQL tables into the Bronze/raw landing path")
    parser.add_argument("--table", required=True, help="PostgreSQL table to ingest")
    parser.add_argument("--limit", type=int, default=100, help="Number of rows to fetch")
    parser.add_argument("--output-dir", default="bronze/raw/postgres", help="Output folder for Bronze raw data")
    parser.add_argument("--bucket", default=None, help="Optional AWS S3 bucket override for real Bronze uploads")
    return parser.parse_args()


def main():
    args = parse_args()
    rows = fetch_table_rows(args.table, limit=args.limit)
    persist_rows(args.table, rows, output_dir=args.output_dir, bucket_name=args.bucket)


if __name__ == "__main__":
    main()
