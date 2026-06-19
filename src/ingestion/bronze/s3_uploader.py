import json
from datetime import datetime, timezone
from typing import Any

from src.utils.s3 import build_s3_client


def upload_records_to_s3(
    records: list[dict[str, Any]], 
    source_name: str, 
    bucket_name: str | None = None
) -> str:
    """Upload raw records to S3 in the Bronze layer structure."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M-%S")
    key = f"bronze/raw/{source_name}/{source_name}_{timestamp}.json"
    
    payload = {
        "source": source_name,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "records": records
    }

    bucket = bucket_name
    if not bucket:
        from config.settings import AWS_S3_BUCKET_NAME
        bucket = AWS_S3_BUCKET_NAME

    client = build_s3_client()
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{bucket}/{key}"
