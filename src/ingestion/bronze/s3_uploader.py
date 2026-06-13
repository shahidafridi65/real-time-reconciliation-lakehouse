import json
from datetime import datetime, timezone
from typing import Any

from src.utils.s3 import upload_json_to_s3


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
    
    return upload_json_to_s3(payload, key, bucket_name)
