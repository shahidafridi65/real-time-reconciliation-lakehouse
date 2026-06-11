import json

from src.transformations.silver.runner import transform_bronze_directory
from src.transformations.silver.transformer import normalize_records, write_silver_dataset


def test_normalize_records_adds_metadata_and_flattens_payload():
    raw_records = [
        {"event_id": "e-1", "event_type": "purchase", "amount": 10.5},
        {"shipment_id": "s-1", "status": "delivered", "tracking": {"carrier": "UPS"}},
    ]

    cleaned = normalize_records(raw_records, source_name="shipping")

    assert cleaned[0]['source_name'] == 'shipping'
    assert cleaned[0]['event_id'] == 'e-1'
    assert cleaned[1]['tracking_carrier'] == 'UPS'


def test_write_silver_dataset_writes_json_file(tmp_path):
    records = [{"event_id": "e-1", "source_name": "clickstream"}]

    output_path = write_silver_dataset(records, output_dir=tmp_path, dataset_name='clickstream')

    assert output_path.endswith('clickstream.json')
    assert json.loads(tmp_path.joinpath('clickstream.json').read_text(encoding='utf-8')) == records


def test_transform_bronze_directory_creates_silver_outputs(tmp_path):
    bronze_dir = tmp_path / 'bronze' / 'raw'
    bronze_dir.mkdir(parents=True)
    bronze_dir.joinpath('orders.json').write_text(json.dumps([{"order_id": "o-1", "customer": {"name": "Ada"}}]), encoding='utf-8')

    output_dir = tmp_path / 'silver'
    written_files = transform_bronze_directory(input_dir=bronze_dir, output_dir=output_dir)

    assert written_files
    assert output_dir.joinpath('orders.json').exists()
