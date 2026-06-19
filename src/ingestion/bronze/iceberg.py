from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_S3_BUCKET_NAME,
    AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
)

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "RTRL_Bronze_Iceberg") -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ]

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[1]")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{AWS_S3_BUCKET_NAME}/iceberg-warehouse/")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.glue.region", AWS_REGION)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
    )

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
            .config("spark.sql.catalog.glue_catalog.s3.access-key-id", AWS_ACCESS_KEY_ID)
            .config("spark.sql.catalog.glue_catalog.s3.secret-access-key", AWS_SECRET_ACCESS_KEY)
        )

    if AWS_SESSION_TOKEN:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.session.token", AWS_SESSION_TOKEN)
            .config("spark.sql.catalog.glue_catalog.s3.session-token", AWS_SESSION_TOKEN)
        )

    return builder.getOrCreate()


def _with_ingestion_metadata(records: list[dict[str, Any]], source_name: str) -> list[dict[str, Any]]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    return [
        {
            **record,
            "_source_name": source_name,
            "_ingested_at": ingested_at,
        }
        for record in records
    ]


def append_records_to_iceberg(
    records: list[dict[str, Any]],
    table_name: str,
    *,
    spark: SparkSession | None = None,
    namespace: str = "bronze",
) -> str:
    if not records:
        logger.info("No records to append for table %s.%s", namespace, table_name)
        return f"glue_catalog.{namespace}.{table_name}"

    owns_session = spark is None
    spark = spark or get_spark_session()

    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue_catalog.{namespace}")
        full_name = f"glue_catalog.{namespace}.{table_name}"
        df = spark.createDataFrame(_with_ingestion_metadata(records, table_name))

        if spark.catalog.tableExists(full_name):
            table_schema = spark.table(full_name).schema
            for field in table_schema:
                if field.name not in df.columns:
                    df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
                else:
                    df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
            df = df.select([field.name for field in table_schema])
            df.writeTo(full_name).append()
        else:
            df.writeTo(full_name).using("iceberg").create()

        return full_name
    finally:
        if owns_session:
            spark.stop()
