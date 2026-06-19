import requests
from typing import Any

from config.settings import MOCK_SHIPPING_API_HOST, MOCK_SHIPPING_API_PORT
from src.ingestion.bronze import s3_uploader


def persist_payload(payload: dict[str, Any], bucket_name: str | None = None) -> str:
    """Fetch shipping data from API and upload to S3 Bronze layer."""
    if payload and "shipments" in payload:
        return s3_uploader.upload_records_to_s3(payload.get("shipments", []), "shipping_payload", bucket_name)
    
    host = "localhost" if MOCK_SHIPPING_API_HOST in {"0.0.0.0", "::"} else MOCK_SHIPPING_API_HOST
    api_url = f"http://{host}:{MOCK_SHIPPING_API_PORT}/shipments"

    try:
        response = requests.get(api_url, params={"limit": 50})
        response.raise_for_status()
        
        shipping_data = response.json()
        records = shipping_data.get("shipments", [])
        
        return s3_uploader.upload_records_to_s3(records, "shipping_payload", bucket_name)
        
    except requests.RequestException as e:
        raise Exception(f"Failed to fetch shipping data: {e}")


def ingest_shipments(bucket_name: str | None = None) -> str:
    """Ingest shipment data from Shipping API to Bronze layer."""
    return persist_payload({}, bucket_name)
