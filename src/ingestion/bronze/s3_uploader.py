import json
from datetime import datetime, timezone

import boto3

from config.settings import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_S3_BUCKET_NAME, AWS_S3_PREFIX, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN


def build_s3_client():
    """Create an AWS S3 client using environment variables for real cloud uploads."""
    if not AWS_S3_BUCKET_NAME:
        raise ValueError("AWS_S3_BUCKET_NAME is not configured. Set it in your .env file.")

    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID or None,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY or None,
        aws_session_token=AWS_SESSION_TOKEN or None,
    )


def upload_records_to_s3(records, source_name: str, bucket_name: str | None = None):
    """Upload raw JSON records directly to an AWS S3 Bronze path.

    This uses standard .json files so the output opens cleanly in AWS Console,
    VS Code, and most JSON viewers.
    """
    bucket = bucket_name or AWS_S3_BUCKET_NAME
    if not bucket:
        raise ValueError("AWS_S3_BUCKET_NAME is required for Bronze uploads.")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    key = f"{AWS_S3_PREFIX}/{source_name}/{source_name}_{timestamp}.json"

    payload = json.dumps(records, ensure_ascii=False, indent=2)

    client = build_s3_client()
    client.put_object(Bucket=bucket, Key=key, Body=payload.encode("utf-8"), ContentType="application/json")

    return f"s3://{bucket}/{key}"
