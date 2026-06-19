import pytest

from src.ingestion.bronze.schemas import RAW_SCHEMAS, validate_records


def test_raw_schemas_cover_all_phase2_sources():
    assert set(RAW_SCHEMAS) == {"clickstream_events", "server_logs", "order_changes", "shipment_data"}


def test_validate_records_accepts_expected_phase2_shapes():
    clickstream = validate_records(
        "clickstream_events",
        [{"event_id": "evt-1", "event_type": "view", "user_id": 101, "event_time": "2026-06-11T10:00:00Z"}],
    )
    server_log = validate_records("server_logs", ["2026-06-11T10:00:00Z 127.0.0.1 api-gateway GET /home 200 100ms"])

    assert clickstream[0]["event_id"] == "evt-1"
    assert server_log[0]["message"].startswith("2026-06-11T10:00:00Z")


def test_validate_records_rejects_invalid_payloads():
    with pytest.raises(ValueError, match="Invalid record"):
        validate_records("shipment_data", [{"tracking_number": "TRK123"}])
