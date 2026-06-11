import argparse
import logging
from pathlib import Path

from src.transformations.silver.transformer import transform_bronze_file

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("silver_runner")


def transform_bronze_directory(input_dir: str | Path = "bronze/raw", output_dir: str | Path = "silver"):
    """Transform all Bronze JSON files into Silver-ready JSON datasets."""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        logger.warning("Bronze input folder does not exist: %s", input_path)
        return []

    written_files = []

    for file_path in sorted(input_path.rglob("*.json")):
        relative_path = file_path.relative_to(input_path)
        dataset_name = relative_path.with_suffix("").as_posix().replace("/", "_")
        target_dir = output_path / relative_path.parent
        written_files.append(transform_bronze_file(file_path, output_dir=target_dir, dataset_name=dataset_name))

    logger.info("Transformed %d Bronze file(s) into Silver datasets", len(written_files))
    return written_files


def parse_args():
    parser = argparse.ArgumentParser(description="Run the Silver transformation layer")
    parser.add_argument("--input-dir", default="bronze/raw", help="Bronze raw directory")
    parser.add_argument("--output-dir", default="silver", help="Silver output directory")
    return parser.parse_args()


def main():
    args = parse_args()
    transform_bronze_directory(input_dir=args.input_dir, output_dir=args.output_dir)


if __name__ == "__main__":
    main()
