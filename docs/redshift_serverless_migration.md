# Amazon Redshift Serverless migration guide

This project now targets Amazon Redshift Serverless for Silver and Gold dbt models. Bronze remains in S3 as Apache Iceberg tables registered in AWS Glue Data Catalog.

## Current readiness assessment

The project is not production-ready yet. It is a solid portfolio/prototype, but several implementation gaps remain:

- Bronze streaming writes use Iceberg and Glue, but `_ensure_iceberg_table` currently drops and recreates tables on startup. That is not safe for production because it can destroy table history and data.
- Bronze batch PostgreSQL/API ingestion still writes JSON payloads to S3; those datasets must also be written or registered as Glue/Iceberg tables before Redshift/dbt can read them as `bronze.users`, `bronze.products`, and `bronze.shipment_data`.
- There is no orchestrator DAG checked in even though the README mentions Airflow.
- The checked-in tests previously referenced a missing Bronze runner and did not execute end to end.
- The former Silver SQL used Snowflake-oriented types such as `STRING`; these have now been converted to Redshift-compatible SQL.
- `.env.example` previously contained real-looking AWS credentials. Rotate those credentials if they were valid.

## AWS resources to create

Use one AWS Region for S3, Glue, Spark, and Redshift. The examples below use `us-east-1`.

1. Create or reuse an S3 bucket, for example `real-time-reconciliation-bronze`.
2. Create a Glue Data Catalog database named `bronze`.
3. Run the Bronze Spark ingestion so Iceberg tables are registered in Glue under database `bronze`.
4. Create an IAM role for Redshift Serverless and attach it to the Redshift namespace.

Minimum Redshift role permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::real-time-reconciliation-bronze",
        "arn:aws:s3:::real-time-reconciliation-bronze/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartition",
        "glue:GetPartitions"
      ],
      "Resource": "*"
    }
  ]
}
```

Trust policy principal:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

## Create Redshift Serverless

1. Open Amazon Redshift console, then create a Redshift Serverless namespace and workgroup.
2. Choose a VPC with three subnets in three availability zones and enough free IP addresses.
3. Set the database name to `dev`, or update `REDSHIFT_DATABASE`.
4. Attach the Redshift IAM role above as the namespace default role.
5. Enable public access only if you need to connect from your laptop. Otherwise connect from a host inside the VPC.
6. In the workgroup security group, allow inbound TCP `5439` from your client IP or trusted private network.
7. Save the workgroup endpoint. It becomes `REDSHIFT_HOST`.

AWS documents the Serverless flow as namespace plus workgroup creation, and notes the VPC/subnet requirement. Redshift can query Glue-cataloged Iceberg tables and join them with local Redshift tables.

## Bootstrap the database

Run this from Query Editor v2 or a SQL client:

```sql
-- Use sql/redshift_bootstrap.sql
CREATE EXTERNAL SCHEMA IF NOT EXISTS bronze
FROM DATA CATALOG
DATABASE 'bronze'
IAM_ROLE default
REGION 'us-east-1';

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

Validate the external tables:

```sql
SELECT schemaname, tablename
FROM svv_external_tables
WHERE schemaname = 'bronze'
ORDER BY tablename;
```

Expected Bronze tables for this dbt project:

- `clickstream`
- `server_logs`
- `order_changes`
- `shipment_data`
- `users`
- `products`

The dbt source maps logical `orders` to physical Iceberg table `order_changes`, and logical `clickstream_events` to physical Iceberg table `clickstream`.

## Local environment

Install dependencies:

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Create `.env` from `.env.example` and set:

```bash
AWS_REGION=us-east-1
AWS_S3_BUCKET_NAME=real-time-reconciliation-bronze
REDSHIFT_HOST=<workgroup-endpoint>
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=dev
REDSHIFT_USER=<database-user>
REDSHIFT_PASSWORD=<database-password>
```

For local dbt, either keep `profiles.yml` in the repo and pass `--profiles-dir .`, or copy it to your user dbt profiles directory.

## Run the pipeline

Start local source services:

```bash
docker compose up -d
python src/simulators/postgres_seeder.py
python src/simulators/shipping_api_mock.py
python src/producers/clickstream_producer.py
python src/producers/server_log_producer.py
```

Run Bronze streaming ingestion to S3 Iceberg/Glue:

```bash
python src/ingestion/bronze/spark_ingest.py
```

For one-shot raw S3 ingestion from Kafka/PostgreSQL/API:

```bash
python -m src.ingestion.bronze.runner --bucket real-time-reconciliation-bronze --max-messages 100
```

Before production use, make the PostgreSQL/API batch outputs Iceberg tables in Glue as well; otherwise Redshift external schema will not expose `users` and `products` to dbt.

Run dbt against Redshift:

```bash
python scripts/preflight_check.py
dbt debug --profiles-dir .
dbt run --profiles-dir . --select silver
dbt test --profiles-dir . --select silver
dbt run --profiles-dir . --select gold
dbt test --profiles-dir . --select gold
```

## Outputs

- Bronze outputs remain in S3 under the Iceberg warehouse path and Glue database `bronze`.
- Silver dbt tables are created in Redshift schema `silver`.
- Gold marts are created in Redshift schema `gold`.

Validate row counts:

```sql
SELECT COUNT(*) FROM silver.silver_orders;
SELECT COUNT(*) FROM silver.silver_clickstream;
SELECT COUNT(*) FROM silver.silver_users;
SELECT COUNT(*) FROM silver.silver_products;
SELECT COUNT(*) FROM silver.silver_shipping_status;
SELECT COUNT(*) FROM silver.silver_server_logs;
```

Validate reconciliation outputs:

```sql
SELECT clickstream_match_status, COUNT(*)
FROM gold.gold_order_reconciliation
GROUP BY 1;

SELECT carrier, AVG(delivery_latency_hours), SUM(CASE WHEN sla_breach THEN 1 ELSE 0 END)
FROM gold.gold_shipping_performance
GROUP BY 1;
```

## Production hardening checklist

- Remove table drops from `src/ingestion/bronze/spark_ingest.py`.
- Add checkpoint lifecycle policies and monitoring for Spark structured streaming.
- Convert all batch sources to Iceberg tables in Glue, not only JSON files in S3.
- Add Airflow, Step Functions, or MWAA orchestration.
- Use Secrets Manager or IAM authentication instead of plain Redshift passwords in `.env`.
- Add dbt freshness tests for Bronze sources.
- Add Redshift WLM/query monitoring and cost alerts.
- Add CI that runs Python tests and `dbt compile` on every change.
