from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_DIR = "{{ var.value.get('rtrl_project_dir', 'E:/real-time-reconciliation-lakehouse') }}"
PYTHON_BIN = "{{ var.value.get('rtrl_python_bin', 'E:/real-time-reconciliation-lakehouse/venv/Scripts/python.exe') }}"
DBT_BIN = "{{ var.value.get('rtrl_dbt_bin', 'E:/real-time-reconciliation-lakehouse/venv/Scripts/python.exe -m dbt.cli.main') }}"
BRONZE_BUCKET = "{{ var.value.get('rtrl_bronze_bucket', '') }}"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
}


with DAG(
    dag_id="reconciliation_lakehouse_pipeline",
    description="Bronze ingestion, Silver transforms, Gold marts, and dbt validation.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "reconciliation", "dbt", "redshift"],
) as dag:
    start = EmptyOperator(task_id="start")

    seed_postgres = BashOperator(
        task_id="seed_postgres_sources",
        bash_command=f'cd "{PROJECT_DIR}" && "{PYTHON_BIN}" src/simulators/postgres_seeder.py',
    )

    produce_clickstream_sample = BashOperator(
        task_id="produce_clickstream_sample",
        bash_command=f'cd "{PROJECT_DIR}" && "{PYTHON_BIN}" src/producers/clickstream_producer.py --max-events 100 --interval 0.1',
    )

    produce_server_log_sample = BashOperator(
        task_id="produce_server_log_sample",
        bash_command=f'cd "{PROJECT_DIR}" && "{PYTHON_BIN}" src/producers/server_log_producer.py --max-events 100 --interval 0.1',
    )

    bronze_batch_ingestion = BashOperator(
        task_id="bronze_batch_ingestion",
        bash_command=(
            f'cd "{PROJECT_DIR}" && "{PYTHON_BIN}" -m src.ingestion.bronze.runner '
            f'--bucket "{BRONZE_BUCKET}" --max-messages 100 --postgres-limit 1000 --write-iceberg'
        ),
    )

    dbt_debug = BashOperator(
        task_id="dbt_debug_redshift",
        bash_command=f'cd "{PROJECT_DIR}" && {DBT_BIN} debug --profiles-dir .',
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f'cd "{PROJECT_DIR}" && {DBT_BIN} run --profiles-dir . --select silver',
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f'cd "{PROJECT_DIR}" && {DBT_BIN} source freshness --profiles-dir .',
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f'cd "{PROJECT_DIR}" && {DBT_BIN} test --profiles-dir . --select silver',
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f'cd "{PROJECT_DIR}" && {DBT_BIN} run --profiles-dir . --select gold',
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=f'cd "{PROJECT_DIR}" && {DBT_BIN} test --profiles-dir . --select gold',
    )

    finish = EmptyOperator(task_id="finish")

    start >> seed_postgres >> [produce_clickstream_sample, produce_server_log_sample]
    [produce_clickstream_sample, produce_server_log_sample] >> bronze_batch_ingestion
    bronze_batch_ingestion >> dbt_debug >> dbt_source_freshness >> dbt_run_silver >> dbt_test_silver
    dbt_test_silver >> dbt_run_gold >> dbt_test_gold >> finish
