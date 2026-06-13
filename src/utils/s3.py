import json
from datetime import datetime, timezone
import boto3
from config.settings import (
    AWS_ACCESS_KEY_ID, AWS_REGION, AWS_S3_BUCKET_NAME, 
    AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
)

def build_s3_client():
    """Create an AWS S3 client using environment variables."""
    if not AWS_S3_BUCKET_NAME:
        raise ValueError("AWS_S3_BUCKET_NAME is not configured.")

    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID or None,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY or None,
        aws_session_token=AWS_SESSION_TOKEN or None,
    )

def upload_json_to_s3(data, key: str, bucket_name: str | None = None):
    """Upload any JSON-serializable data to a specific S3 key."""
    bucket = bucket_name or AWS_S3_BUCKET_NAME
    client = build_s3_client()
    
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    client.put_object(
        Bucket=bucket, 
        Key=key, 
        Body=payload.encode("utf-8"), 
        ContentType="application/json"
    )
    return f"s3://{bucket}/{key}"

def read_json_from_s3(key: str, bucket_name: str | None = None):
    """Read and parse a JSON file from S3."""
    bucket = bucket_name or AWS_S3_BUCKET_NAME
    client = build_s3_client()
    
    response = client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))

def list_s3_objects(prefix: str, bucket_name: str | None = None):
    """List all object keys under a specific S3 prefix."""
    bucket = bucket_name or AWS_S3_BUCKET_NAME
    client = build_s3_client()
    
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                keys.append(obj["Key"])
                
    return keys
