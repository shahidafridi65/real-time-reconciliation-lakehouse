import json
from pathlib import Path
from typing import Any

from src.transformations.silver.transformer import normalize_records, write_silver_dataset


def transform_bronze_directory(input_dir: Path, output_dir: Path) -> list[str]:
    """Transform all JSON files from bronze directory to silver layer."""
    written_files = []
    
    bronze_path = Path(input_dir)
    if not bronze_path.exists():
        return written_files
    
    for json_file in bronze_path.glob("*.json"):
        try:
            # Read raw bronze data
            raw_records = json.loads(json_file.read_text(encoding="utf-8"))
            if isinstance(raw_records, dict):
                raw_records = [raw_records]
            
            # Extract dataset name from filename
            dataset_name = json_file.stem
            
            # Normalize and transform records
            normalized_records = normalize_records(raw_records, source_name=dataset_name)
            
            # Write to silver layer
            output_path = write_silver_dataset(
                normalized_records, 
                output_dir=output_dir, 
                dataset_name=dataset_name
            )
            
            written_files.append(output_path)
            
        except Exception as e:
            print(f"Error processing {json_file}: {e}")
    
    return written_files
