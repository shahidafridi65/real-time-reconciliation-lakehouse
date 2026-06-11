from src.ingestion.bronze import postgres_ingest, shipping_ingest
from src.ingestion.bronze.s3_uploader import upload_records_to_s3


def test_upload_records_to_s3_uses_real_cloud_path(monkeypatch):
    class FakeS3Client:
        def __init__(self):
            self.uploads = []

        def put_object(self, **kwargs):
            self.uploads.append(kwargs)

    fake_client = FakeS3Client()

    monkeypatch.setattr(
        "src.ingestion.bronze.s3_uploader.build_s3_client",
        lambda: fake_client,
    )

    result = upload_records_to_s3(
        records=[{"event_id": "e-1", "event_type": "purchase"}],
        source_name="clickstream_events",
        bucket_name="demo-bronze-bucket",
    )

    assert result.startswith("s3://demo-bronze-bucket/bronze/raw/clickstream_events/")
    assert result.endswith('.json')
    assert fake_client.uploads
    assert fake_client.uploads[0]['Bucket'] == 'demo-bronze-bucket'
    assert fake_client.uploads[0]['ContentType'] == 'application/json'


def test_postgres_ingest_can_upload_raw_rows_to_s3(monkeypatch, tmp_path):
    uploaded = {}

    def fake_upload(records, source_name, bucket_name=None):
        uploaded['records'] = records
        uploaded['source_name'] = source_name
        uploaded['bucket_name'] = bucket_name
        return 's3://demo-bronze-bucket/bronze/raw/postgres/users.json'

    monkeypatch.setattr('src.ingestion.bronze.s3_uploader.upload_records_to_s3', fake_upload)

    result = postgres_ingest.persist_rows(
        'users',
        [{'id': 1, 'name': 'Ada'}],
        output_dir=tmp_path,
        bucket_name='demo-bronze-bucket',
    )

    assert result == 's3://demo-bronze-bucket/bronze/raw/postgres/users.json'
    assert uploaded['source_name'] == 'users'
    assert uploaded['bucket_name'] == 'demo-bronze-bucket'


def test_shipping_ingest_can_upload_payload_to_s3(monkeypatch, tmp_path):
    uploaded = {}

    def fake_upload(records, source_name, bucket_name=None):
        uploaded['records'] = records
        uploaded['source_name'] = source_name
        uploaded['bucket_name'] = bucket_name
        return 's3://demo-bronze-bucket/bronze/raw/shipping/shipping_payload.json'

    monkeypatch.setattr('src.ingestion.bronze.s3_uploader.upload_records_to_s3', fake_upload)

    result = shipping_ingest.persist_payload(
        {'shipments': [{'shipment_id': 's-1', 'order_id': 'o-1', 'status': 'shipped', 'updated_at': '2026-06-11T10:00:00Z'}]},
        output_dir=tmp_path,
        bucket_name='demo-bronze-bucket',
    )

    assert result == 's3://demo-bronze-bucket/bronze/raw/shipping/shipping_payload.json'
    assert uploaded['source_name'] == 'shipping_payload'
    assert uploaded['bucket_name'] == 'demo-bronze-bucket'
