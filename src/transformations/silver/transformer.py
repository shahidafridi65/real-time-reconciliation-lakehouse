import argparse
import json
import logging
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("silver_transformer")


def flatten_mapping(mapping: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten nested dictionaries into a single-level mapping for Silver modeling."""
    flattened: dict[str, Any] = {}

    for key, value in mapping.items():
        new_key = f"{prefix}_{key}" if prefix else key

        if isinstance(value, dict):
            flattened.update(flatten_mapping(value, prefix=new_key))
        else:
            flattened[new_key] = value

    return flattened


def normalize_records(records: Any, source_name: str) -> list[dict[str, Any]]:
    """Normalize Bronze JSON records into a Silver-friendly structure.

    The transformation adds a source name, a stable record index, and flattens
    nested JSON fields to make downstream modelling easier.
    """
    if isinstance(records, dict) and "shipments" in records and isinstance(records["shipments"], list):
        records = records["shipments"]

    if isinstance(records, dict):
        records = [records]

    normalized: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            normalized.append({"value": record, "source_name": source_name, "record_index": index})
            continue

        flat_record = flatten_mapping(record)
        flat_record.setdefault("source_name", source_name)
        flat_record.setdefault("record_index", index)
        normalized.append(flat_record)

    return normalized


def load_json_records(file_path: str | Path) -> Any:
    """Load a JSON file from Bronze or Silver storage."""
    path = Path(file_path)
    return json.loads(path.read_text(encoding="utf-8"))


def write_silver_dataset(records: list[dict[str, Any]], output_dir: str | Path, dataset_name: str) -> str:
    """Write normalized Silver records as a JSON dataset in the Silver landing path."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / f"{dataset_name}.json"
    file_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("Wrote %d Silver records to %s", len(records), file_path)
    return str(file_path)


def transform_bronze_file(input_path: str | Path, output_dir: str | Path, dataset_name: str) -> str:
    """Transform a Bronze JSON file into a Silver JSON dataset."""
    raw_payload = load_json_records(input_path)

    if isinstance(raw_payload, dict) and "shipments" in raw_payload:
        source_name = "shipping"
    else:
        source_name = Path(input_path).stem.split("_")[0] if isinstance(input_path, (str, Path)) else "bronze"

    records = normalize_records(raw_payload, source_name=source_name)
    return write_silver_dataset(records, output_dir=output_dir, dataset_name=dataset_name)


def parse_args():
    parser = argparse.ArgumentParser(description="Transform Bronze JSON data into Silver-ready datasets")
    parser.add_argument("--input", required=True, help="Bronze JSON file to transform")
    parser.add_argument("--output-dir", default="silver", help="Silver output folder")
    parser.add_argument("--dataset-name", default=None, help="Optional Silver dataset name override")
    return parser.parse_args()


def main():
    args = parse_args()
    dataset_name = args.dataset_name or Path(args.input).stem
    transform_bronze_file(args.input, output_dir=args.output_dir, dataset_name=dataset_name)


if __name__ == "__main__":
    main()
