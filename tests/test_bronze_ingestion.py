import json

from pathlib import Path

from src.ingestion.bronze.raw_writer import persist_raw_records


def test_persist_raw_records_writes_json_lines(tmp_path):
    output_dir = tmp_path / "bronze" / "raw"

    written_files = persist_raw_records(
        source_name="clickstream_events",
        records=[{"event_id": "e-1", "event_type": "purchase"}],
        output_dir=output_dir,
    )

    assert len(written_files) == 1

    file_path = written_files[0]
    assert Path(file_path).exists()

    content = Path(file_path).read_text(encoding="utf-8").strip().splitlines()
    assert len(content) == 1
    assert json.loads(content[0])["event_id"] == "e-1"
