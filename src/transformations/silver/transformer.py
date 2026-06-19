import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def normalize_records(records: list[dict[str, Any]], source_name: str) -> list[dict[str, Any]]:
    """Normalize raw records by adding metadata and flattening nested JSON."""
    normalized = []
    
    for record in records:
        normalized_record = {
            "source_name": source_name,
            "_transformed_at": datetime.now(timezone.utc).isoformat(),
        }
        
        # Flatten nested JSON structures
        for key, value in record.items():
            if isinstance(value, dict):
                # Flatten nested dictionaries
                for nested_key, nested_value in value.items():
                    normalized_record[f"{key}_{nested_key}"] = nested_value
            else:
                normalized_record[key] = value
        
        normalized.append(normalized_record)
    
    return normalized


def write_silver_dataset(
    records: list[dict[str, Any]], 
    output_dir: Path, 
    dataset_name: str
) -> str:
    """Write transformed records to a JSON file in the silver layer."""
    output_path = Path(output_dir) / f"{dataset_name}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    output_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(output_path)
