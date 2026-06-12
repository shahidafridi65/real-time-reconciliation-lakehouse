from typing import Any

from sqlalchemy import text

from src.ingestion.bronze.s3_uploader import upload_records_to_s3
from src.utils.db import engine


def persist_rows(table_name: str, records: list[dict[str, Any]], bucket_name: str | None = None) -> str:
    """Read rows from PostgreSQL table and upload to S3 Bronze layer."""
    rows = []
    
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table_name}"))
        columns = result.keys()
        
        for row in result:
            row_dict = dict(zip(columns, row))
            rows.append(row_dict)
    
    return upload_records_to_s3(rows, table_name, bucket_name)


def ingest_users(bucket_name: str | None = None) -> str:
    """Ingest users table from PostgreSQL to Bronze layer."""
    return persist_rows("users", [], bucket_name)


def ingest_products(bucket_name: str | None = None) -> str:
    """Ingest products table from PostgreSQL to Bronze layer."""
    return persist_rows("products", [], bucket_name)


def ingest_orders(bucket_name: str | None = None) -> str:
    """Ingest orders table from PostgreSQL to Bronze layer."""
    return persist_rows("orders", [], bucket_name)
