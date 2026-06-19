from __future__ import annotations

import importlib.util
import socket
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.settings import (
    AWS_REGION,
    AWS_S3_BUCKET_NAME,
    DATABASE_URL,
    KAFKA_BOOTSTRAP_SERVERS,
    REDSHIFT_DATABASE,
    REDSHIFT_HOST,
    REDSHIFT_PASSWORD,
    REDSHIFT_PORT,
    REDSHIFT_USER,
)


REQUIRED_FILES = [
    "dbt_project.yml",
    "profiles.yml",
    "sql/redshift_bootstrap.sql",
    "src/ingestion/bronze/spark_ingest.py",
    "src/ingestion/bronze/runner.py",
    "models/silver/silver_orders.sql",
    "models/gold/gold_order_reconciliation.sql",
    "orchestration/airflow/dags/reconciliation_lakehouse_dag.py",
]


def _mask(value: str) -> str:
    if not value:
        return "MISSING"
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}***{value[-4:]}"


def check_files(root: Path) -> list[str]:
    failures = []
    for relative_path in REQUIRED_FILES:
        if not root.joinpath(relative_path).exists():
            failures.append(f"Missing required file: {relative_path}")
    return failures


def check_python_packages() -> list[str]:
    failures = []
    for module_name in ["boto3", "kafka", "pyspark", "dbt", "redshift_connector"]:
        if importlib.util.find_spec(module_name) is None:
            failures.append(f"Missing Python package/module: {module_name}")
    return failures


def check_tcp(host: str, port: int, timeout_seconds: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


def main() -> int:
    root = ROOT_DIR
    failures: list[str] = []

    failures.extend(check_files(root))
    failures.extend(check_python_packages())

    required_env = {
        "DATABASE_URL": DATABASE_URL,
        "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
        "AWS_REGION": AWS_REGION,
        "AWS_S3_BUCKET_NAME": AWS_S3_BUCKET_NAME,
        "REDSHIFT_HOST": REDSHIFT_HOST,
        "REDSHIFT_PORT": str(REDSHIFT_PORT),
        "REDSHIFT_DATABASE": REDSHIFT_DATABASE,
        "REDSHIFT_USER": REDSHIFT_USER,
        "REDSHIFT_PASSWORD": REDSHIFT_PASSWORD,
    }

    for name, value in required_env.items():
        if not value:
            failures.append(f"Missing environment variable: {name}")

    print("Preflight configuration summary")
    print(f"  DATABASE_URL: {_mask(DATABASE_URL or '')}")
    print(f"  KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS or 'MISSING'}")
    print(f"  AWS_REGION: {AWS_REGION or 'MISSING'}")
    print(f"  AWS_S3_BUCKET_NAME: {AWS_S3_BUCKET_NAME or 'MISSING'}")
    print(f"  REDSHIFT_HOST: {REDSHIFT_HOST or 'MISSING'}")
    print(f"  REDSHIFT_PORT: {REDSHIFT_PORT}")
    print(f"  REDSHIFT_DATABASE: {REDSHIFT_DATABASE or 'MISSING'}")
    print(f"  REDSHIFT_USER: {REDSHIFT_USER or 'MISSING'}")
    print(f"  REDSHIFT_PASSWORD: {_mask(REDSHIFT_PASSWORD)}")

    if REDSHIFT_HOST:
        redshift_reachable = check_tcp(REDSHIFT_HOST, REDSHIFT_PORT)
        print(f"  REDSHIFT_TCP_REACHABLE: {redshift_reachable}")
        if not redshift_reachable:
            failures.append(
                "Cannot reach Redshift host/port. Check public access, VPC, security group inbound 5439, "
                "and whether your network allows outbound 5439."
            )

    if failures:
        print("\nPreflight failed:")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("\nPreflight passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
