# Real-Time Reconciliation Lakehouse

An end-to-end data engineering project that simulates a modern e-commerce platform and builds a real-time lakehouse for clickstream analytics, operational data ingestion, cross-system reconciliation, and business reporting.

## Project overview

E-commerce platforms generate data across multiple systems at the same time: website activity, backend databases, third-party logistics providers, and application logs. In practice, these systems often drift apart. Orders can be missing, duplicate events can appear, shipping updates may arrive late, and operational teams may not have one trusted source of truth.

This project solves that problem by building a unified data platform that ingests both streaming and batch data, lands it into a lakehouse architecture, transforms it into analytics-ready models, and exposes curated outputs for reconciliation, customer analytics, and operational monitoring.

The project is designed around three business goals:

- **Customer behavior analytics** — understand what users are viewing, searching, adding to cart, and purchasing in near real time.
- **Financial and operational reconciliation** — detect mismatches between user activity, backend orders, and system logs.
- **Historical auditability** — preserve clean, queryable records for downstream reporting and investigation.

## What this project demonstrates

- Real-time event ingestion with Kafka
- Batch and API-based ingestion patterns
- Medallion lakehouse architecture: Bronze, Silver, Gold
- Data cleaning, flattening, and deduplication
- Reconciliation logic across multiple systems
- Business-ready marts for analytics and monitoring
- Production-oriented pipeline design with orchestration and BI consumption

## Architecture summary

The platform combines four source systems:

- **Clickstream events** from user interactions on the storefront
- **PostgreSQL operational data** for users, products, and orders
- **Third-party shipping API** data in nested JSON format
- **Server application logs** representing backend system activity

These sources flow into a centralized lakehouse where raw data is ingested, standardized, reconciled, and transformed into curated analytical models for downstream reporting and dashboards.

## Tech stack

| Layer | Tools |
|---|---|
| Programming | Python, SQL |
| Streaming | Apache Kafka |
| Operational Database | PostgreSQL |
| API Mocking | FastAPI |
| Processing | PySpark Structured Streaming |
| Storage | AWS S3 |
| Table Format | Apache Iceberg |
| Warehouse | Amazon Redshift Serverless |
| Transformations | dbt |
| Orchestration | Apache Airflow |
| Visualization | Power BI / Tableau |
| Local Infrastructure | Docker, Docker Compose |


### 1. Clone the repository
```bash
git clone https://github.com/<your-username>/real-time-reconciliation-lakehouse.git
cd real-time-reconciliation-lakehouse
```

### 2. Create and activate a virtual environment
```bash
python -m venv venv
source venv/bin/activate
```

For Windows:

```bash
venv\\Scripts\\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Start local infrastructure
```bash
docker compose up -d
```

This starts:
- PostgreSQL
- Zookeeper
- Kafka

### 5. Seed the PostgreSQL source database
```bash
python src/simulators/postgres_seeder.py
```

### 6. Start the mock shipping API
```bash
python src/simulators/shipping_api_mock.py
```

### 7. Start streaming source generators
Run these in separate terminals:

```bash
python src/producers/clickstream_producer.py
```

```bash
python src/producers/server_log_producer.py
```

## Redshift Serverless migration

Silver and Gold dbt models now target Amazon Redshift Serverless instead of Snowflake. See `docs/redshift_serverless_migration.md` for AWS setup, Redshift external schema bootstrap, dbt commands, validation SQL, and production hardening notes.

## Gold marts and orchestration

Phase 4 Gold marts and Phase 5 validation/orchestration assets are documented in `docs/phase4_phase5_runbook.md`. The Airflow-style DAG is available at `orchestration/airflow/dags/reconciliation_lakehouse_dag.py`.

## Execution guide

Use `docs/project_execution_workflow.md` for the production-ready command sequence, expected outputs, validation SQL, workflow explanation, and architecture diagram.

## Source systems produced locally

After setup, the project will expose these local sources:

- **Kafka topic:** `clickstream_events`
- **Kafka topic:** `server_logs`
- **PostgreSQL tables:** `users`, `products`, `orders`
- **Mock REST API:** `http://localhost:8000/shipments`

## Example business questions this platform can answer

- Which products are users interacting with right now?
- Did every purchase event successfully persist into the order system?
- Are there backend failures or timeout-related drops?
- Which carriers have the worst delivery latency?
- How can operations and finance teams monitor discrepancies from one trusted layer?

## Why this repository stands out

This project is built as a portfolio-grade data platform rather than a single ETL script. It combines streaming, batch ingestion, API ingestion, reconciliation logic, warehouse modeling, and business-facing outputs in one realistic architecture.

It is designed to demonstrate both technical depth and business understanding:
- building reliable pipelines,
- modeling data for trust and auditability,
- and solving problems that matter to analytics, operations, and finance teams.
