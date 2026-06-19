from __future__ import annotations

import logging

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from config.settings import AWS_S3_BUCKET_NAME, KAFKA_BOOTSTRAP_SERVERS
from src.ingestion.bronze.iceberg import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_spark_ingest")

LOG_PATTERN = r"^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\d+)\s+(\d+)ms$"


def _ensure_namespace(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS glue_catalog.bronze")


def _create_table_if_missing(spark: SparkSession, table_name: str, sql_schema: str) -> None:
    _ensure_namespace(spark)
    full_name = f"glue_catalog.bronze.{table_name}"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_name} ({sql_schema}) USING iceberg")
    logger.info("Iceberg table is ready without destructive drops: %s", full_name)


def ingest_kafka_json_to_iceberg(
    spark: SparkSession,
    topic: str,
    table_name: str,
    schema: StructType,
    sql_schema: str,
):
    _create_table_if_missing(spark, table_name, sql_schema)
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_df.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
        .withColumn("_source_name", F.lit(table_name))
        .withColumn("_ingested_at", current_timestamp())
    )

    return (
        parsed_df.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", f"s3a://{AWS_S3_BUCKET_NAME}/checkpoints/bronze/{table_name}")
        .toTable(f"glue_catalog.bronze.{table_name}")
    )


def ingest_server_logs_to_iceberg(spark: SparkSession):
    table_name = "server_logs"
    _create_table_if_missing(
        spark,
        table_name,
        """
        timestamp STRING,
        ip STRING,
        service STRING,
        method STRING,
        path STRING,
        status_code INT,
        latency_ms INT,
        _source_name STRING,
        _ingested_at TIMESTAMP
        """,
    )

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "server_logs")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS raw_line")
    )

    parsed_df = (
        raw_df.select(
            F.regexp_extract("raw_line", LOG_PATTERN, 1).alias("timestamp"),
            F.regexp_extract("raw_line", LOG_PATTERN, 2).alias("ip"),
            F.regexp_extract("raw_line", LOG_PATTERN, 3).alias("service"),
            F.regexp_extract("raw_line", LOG_PATTERN, 4).alias("method"),
            F.regexp_extract("raw_line", LOG_PATTERN, 5).alias("path"),
            F.regexp_extract("raw_line", LOG_PATTERN, 6).cast("int").alias("status_code"),
            F.regexp_extract("raw_line", LOG_PATTERN, 7).cast("int").alias("latency_ms"),
            F.lit(table_name).alias("_source_name"),
            F.current_timestamp().alias("_ingested_at"),
        ).filter(F.col("timestamp") != "")
    )

    return (
        parsed_df.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", f"s3a://{AWS_S3_BUCKET_NAME}/checkpoints/bronze/server_logs")
        .toTable("glue_catalog.bronze.server_logs")
    )


def main() -> None:
    spark = get_spark_session("Bronze_Streaming_Ingestion")

    clickstream_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("country", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
    ])

    order_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("order_status", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("order_placed_at", StringType(), True),
    ])

    shipment_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("carrier", StringType(), True),
        StructField("tracking_number", StringType(), True),
        StructField("status", StringType(), True),
        StructField("status_last_updated_at", StringType(), True),
    ])

    queries = [
        ingest_kafka_json_to_iceberg(
            spark,
            "clickstream_events",
            "clickstream",
            clickstream_schema,
            """
            event_id STRING, event_time STRING, user_id INT, session_id STRING,
            event_type STRING, product_id INT, category STRING, device_type STRING,
            country STRING, price DOUBLE, quantity INT, _source_name STRING, _ingested_at TIMESTAMP
            """,
        ),
        ingest_server_logs_to_iceberg(spark),
        ingest_kafka_json_to_iceberg(
            spark,
            "order_changes",
            "order_changes",
            order_schema,
            """
            order_id INT, user_id INT, product_id INT, quantity INT, total_amount DOUBLE,
            order_status STRING, payment_status STRING, order_placed_at STRING,
            _source_name STRING, _ingested_at TIMESTAMP
            """,
        ),
        ingest_kafka_json_to_iceberg(
            spark,
            "shipment_data",
            "shipment_data",
            shipment_schema,
            """
            order_id INT, carrier STRING, tracking_number STRING, status STRING,
            status_last_updated_at STRING, _source_name STRING, _ingested_at TIMESTAMP
            """,
        ),
    ]

    logger.info("Bronze streaming queries started: %s", len(queries))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
