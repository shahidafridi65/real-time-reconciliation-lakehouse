# Phase 4 and Phase 5 runbook

## Phase 4 outputs

Gold marts are built with dbt:

- `gold.gold_order_reconciliation` detects missing purchase events, delayed purchase events, payment mismatches, gateway timeouts near order placement, and purchase events without matching orders.
- `gold.gold_logistics_performance` tracks shipment coverage, carrier delay status, delivery latency, SLA breaches, and missing shipments.
- `gold.gold_customer_summary` provides lifetime spend, order count, activity, favorite category, failed payment count, and customer segmentation.

Backward-compatible aliases remain:

- `gold.gold_shipping_performance`
- `gold.gold_dim_customers`

## Phase 5 validation

dbt schema tests cover nulls, uniqueness, accepted values, and Gold mart keys. Singular dbt tests under `tests/dbt` enforce:

- Bronze source freshness warnings after 2 hours and failures after 6 hours,
- reconciliation exception rate threshold,
- no negative delivery latency,
- non-negative customer metrics,
- payment mismatch status alignment.

Python validation helpers live in `src/validation/data_quality.py` for local unit tests and non-warehouse checks.

## Manual execution

Run local tests:

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
python -m pytest -q -p no:cacheprovider
```

Run preflight before cloud execution:

```powershell
python scripts/preflight_check.py
```

Parse dbt with the Redshift adapter:

```powershell
$env:REDSHIFT_HOST='your-workgroup-endpoint'
$env:REDSHIFT_USER='admin'
$env:REDSHIFT_PASSWORD='your-password'
$env:REDSHIFT_DATABASE='dev'
venv\Scripts\python.exe -m dbt.cli.main parse --profiles-dir .
```

When Redshift Serverless is available:

```powershell
venv\Scripts\python.exe -m dbt.cli.main debug --profiles-dir .
venv\Scripts\python.exe -m dbt.cli.main source freshness --profiles-dir .
venv\Scripts\python.exe -m dbt.cli.main run --profiles-dir . --select silver
venv\Scripts\python.exe -m dbt.cli.main test --profiles-dir . --select silver
venv\Scripts\python.exe -m dbt.cli.main run --profiles-dir . --select gold
venv\Scripts\python.exe -m dbt.cli.main test --profiles-dir . --select gold
```

## Airflow deployment

The DAG is located at `orchestration/airflow/dags/reconciliation_lakehouse_dag.py`.

Set these Airflow Variables:

- `rtrl_project_dir`: project root, for example `E:/real-time-reconciliation-lakehouse`
- `rtrl_python_bin`: Python interpreter path, for example `E:/real-time-reconciliation-lakehouse/venv/Scripts/python.exe`
- `rtrl_dbt_bin`: dbt command, for example `E:/real-time-reconciliation-lakehouse/venv/Scripts/python.exe -m dbt.cli.main`
- `rtrl_bronze_bucket`: S3 Bronze bucket name

Set Redshift and AWS credentials in the Airflow environment or secrets backend:

- `REDSHIFT_HOST`
- `REDSHIFT_PORT`
- `REDSHIFT_DATABASE`
- `REDSHIFT_USER`
- `REDSHIFT_PASSWORD`
- `AWS_REGION`
- `AWS_S3_BUCKET_NAME`

Production deployment should run Airflow/MWAA from a network path that can reach Kafka, PostgreSQL, S3, Glue, and Redshift Serverless.
