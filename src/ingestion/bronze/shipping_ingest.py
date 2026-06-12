import requests
from typing import Any

from config.settings import MOCK_SHIPPING_API_HOST, MOCK_SHIPPING_API_PORT
from src.ingestion.bronze.s3_uploader import upload_records_to_s3


def persist_payload(payload: dict[str, Any], bucket_name: str | None = None) -> str:
    """Fetch shipping data from API and upload to S3 Bronze layer."""
    api_url = f"http://{MOCK_SHIPPING_API_HOST}:{MOCK_SHIPPING_API_PORT}/shipments"
    
    try:
        response = requests.get(api_url, params={"limit": 50})
        response.raise_for_status()
        
        shipping_data = response.json()
        records = shipping_data.get("shipments", [])
        
        return upload_records_to_s3(records, "shipping_payload", bucket_name)
        
    except requests.RequestException as e:
        raise Exception(f"Failed to fetch shipping data: {e}")


def ingest_shipments(bucket_name: str | None = None) -> str:
    """Ingest shipment data from Shipping API to Bronze layer."""
    return persist_payload({}, bucket_name)
