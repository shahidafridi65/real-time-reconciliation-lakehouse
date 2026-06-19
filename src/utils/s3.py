import json
from typing import Any

import boto3

from config.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_S3_BUCKET_NAME,
    AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
)


def build_s3_client():
    if not AWS_S3_BUCKET_NAME:
        raise ValueError("AWS_S3_BUCKET_NAME is not configured.")

    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID or None,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY or None,
        aws_session_token=AWS_SESSION_TOKEN or None,
    )


def upload_json_to_s3(data: Any, key: str, bucket_name: str | None = None) -> str:
    bucket = bucket_name or AWS_S3_BUCKET_NAME
    if not bucket:
        raise ValueError("S3 bucket is not configured.")

    client = build_s3_client()
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{bucket}/{key}"


def read_json_from_s3(key: str, bucket_name: str | None = None) -> Any:
    bucket = bucket_name or AWS_S3_BUCKET_NAME
    client = build_s3_client()
    response = client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def list_s3_objects(prefix: str, bucket_name: str | None = None) -> list[str]:
    bucket = bucket_name or AWS_S3_BUCKET_NAME
    client = build_s3_client()
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys
