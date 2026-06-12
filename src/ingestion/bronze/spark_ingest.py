import logging

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, LongType,
    FloatType, BooleanType, TimestampType, DateType,
)

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    AWS_S3_BUCKET_NAME,
    AWS_REGION,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_spark_ingest")

# ---------------------------------------------------------------------------
# Server log regex: "<ts> <ip> <service> <method> <path> <status_code> <latency>ms"
# ---------------------------------------------------------------------------
_LOG_PATTERN = r"^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\d+)\s+(\d+)ms$"


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
def get_spark_session(app_name: str) -> SparkSession:
    """Build a SparkSession wired to Kafka, Apache Iceberg, AWS Glue, and S3."""

    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ]

    builder = (
        SparkSession.builder
        .appName(app_name)
        # ── Jar resolution ──────────────────────────────────────────────────
        .config("spark.jars.packages", ",".join(packages))
        # ── Windows IPv4 fix ────────────────────────────────────────────────
        .config("spark.driver.extraJavaOptions",   "-Djava.net.preferIPv4Stack=true")
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        # ── Iceberg extension ───────────────────────────────────────────────
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # ── Glue Catalog ────────────────────────────────────────────────────
        .config("spark.sql.catalog.glue_catalog",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse",
                f"s3://{AWS_S3_BUCKET_NAME}/iceberg-warehouse/")
        .config("spark.sql.catalog.glue_catalog.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.glue.region", AWS_REGION)
        # ── S3A credentials ─────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key",        AWS_ACCESS_KEY_ID.strip())
        .config("spark.hadoop.fs.s3a.secret.key",        AWS_SECRET_ACCESS_KEY.strip())
        .config("spark.hadoop.fs.s3a.endpoint",          "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        # ── Iceberg S3 credentials (separate client) ────────────────────────
        .config("spark.sql.catalog.glue_catalog.s3.access-key-id",
                AWS_ACCESS_KEY_ID.strip())
        .config("spark.sql.catalog.glue_catalog.s3.secret-access-key",
                AWS_SECRET_ACCESS_KEY.strip())
    )

    if AWS_SESSION_TOKEN:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.session.token",
                    AWS_SESSION_TOKEN)
            .config("spark.sql.catalog.glue_catalog.s3.session-token",
                    AWS_SESSION_TOKEN)
        )

    return builder.getOrCreate()


def struct_to_sql_schema(schema: StructType) -> str:
    """Convert a PySpark StructType to a Spark SQL DDL column list string."""
    _TYPE_MAP = {
        StringType:    "STRING",
        IntegerType:   "INT",
        DoubleType:    "DOUBLE",
        LongType:      "BIGINT",
        FloatType:     "FLOAT",
        BooleanType:   "BOOLEAN",
        TimestampType: "TIMESTAMP",
        DateType:      "DATE",
    }
    columns = []
    for field in schema.fields:
        sql_type = _TYPE_MAP.get(type(field.dataType), "STRING")
        columns.append(f"`{field.name}` {sql_type}")
    # Audit column appended to every Bronze table
    columns.append("`_ingested_at` TIMESTAMP")
    return ", ".join(columns)


# Helper: ensure Glue namespace + Iceberg table exist before streaming starts
def _ensure_iceberg_table(spark: SparkSession, table_name: str, sql_schema: str) -> None:
    """Create the glue_catalog.bronze namespace and table if they don't exist."""
    full_name = f"glue_catalog.bronze.{table_name}"
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS glue_catalog.bronze")
        
        # Drop table if it exists to avoid schema conflicts
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_name}")
            logger.info("Dropped existing table: %s", full_name)
        except:
            pass
            
        spark.sql(
            f"CREATE TABLE {full_name} ({sql_schema}) USING iceberg"
        )
        logger.info("Iceberg table ready: %s", full_name)
    except Exception as exc:
        logger.warning(
            "Could not pre-create Iceberg table %s — streaming may fail if it "
            "does not already exist in Glue. Error: %s",
            full_name, exc,
        )


# Ingestor A: JSON topics  (clickstream, order_changes, shipment_data)
def ingest_kafka_to_iceberg(
    spark: SparkSession,
    topic: str,
    table_name: str,
    schema: StructType,
) -> "StreamingQuery":
    """
    Read a Kafka topic that carries JSON payloads, parse them with *schema*,
    attach an ingestion timestamp, and write to the Iceberg Bronze table.

    Uses .toTable() — the correct Iceberg structured-streaming sink API.
    """
    logger.info("Preparing Bronze ingest: topic=%s  table=glue_catalog.bronze.%s",
                topic, table_name)

    _ensure_iceberg_table(spark, table_name, struct_to_sql_schema(schema))

    # 1. Read raw Kafka stream
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        # Avoid losing data if Kafka offsets are ahead of what Spark last read
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse JSON → typed columns + audit timestamp
    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
        .withColumn("_ingested_at", current_timestamp())
    )
    
    # 3. Write to Iceberg via .toTable() — correct API for Iceberg sink
    query = (
        parsed_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option(
            "checkpointLocation",
            f"s3a://{AWS_S3_BUCKET_NAME}/checkpoints/bronze/{table_name}",
        )
        .toTable(f"glue_catalog.bronze.{table_name}")
    )

    logger.info("Stream started for topic=%s", topic)
    return query


# Ingestor B: Plain-text server logs
def ingest_server_logs_to_iceberg(spark: SparkSession) -> "StreamingQuery":
    """
    Read plain-text server log lines from Kafka and write structured rows
    to the Iceberg Bronze table glue_catalog.bronze.server_logs.

    Log format: <timestamp> <ip> <service> <method> <path> <status_code> <latency>ms
    Example   : 2024-01-15T12:00:00Z 1.2.3.4 api-gateway GET /home 200 123ms
    """
    logger.info("Preparing Bronze ingest: topic=server_logs (plain-text regex parser)")

    server_logs_ddl = (
        "`timestamp` STRING, `ip` STRING, `service` STRING, `method` STRING, "
        "`path` STRING, `status_code` INT, `latency_ms` INT, `_ingested_at` TIMESTAMP"
    )
    _ensure_iceberg_table(spark, "server_logs", server_logs_ddl)

    # 1. Read raw text lines from Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "server_logs")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS raw_line")
    )

    # 2. Parse each line with regex; rows that don't match are dropped
    parsed_df = (
        raw_df.select(
            F.regexp_extract("raw_line", _LOG_PATTERN, 1).alias("timestamp"),
            F.regexp_extract("raw_line", _LOG_PATTERN, 2).alias("ip"),
            F.regexp_extract("raw_line", _LOG_PATTERN, 3).alias("service"),
            F.regexp_extract("raw_line", _LOG_PATTERN, 4).alias("method"),
            F.regexp_extract("raw_line", _LOG_PATTERN, 5).alias("path"),
            F.regexp_extract("raw_line", _LOG_PATTERN, 6).cast("int").alias("status_code"),
            F.regexp_extract("raw_line", _LOG_PATTERN, 7).cast("int").alias("latency_ms"),
            F.current_timestamp().alias("_ingested_at"),
        )
        .filter(F.col("timestamp") != "")   # discard lines that did not match pattern
    )

    # 3. Write to Iceberg
    query = (
        parsed_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option(
            "checkpointLocation",
            f"s3a://{AWS_S3_BUCKET_NAME}/checkpoints/bronze/server_logs",
        )
        .toTable("glue_catalog.bronze.server_logs")
    )

    logger.info("Stream started for topic=server_logs")
    return query


# Entry point — runs all 4 Bronze ingest streams concurrently
if __name__ == "__main__":

    spark = get_spark_session("Bronze_Ingestion_Runner")

    # ── 1. Clickstream events (JSON) ─────────────────────────────────────────
    clickstream_schema = StructType([
        StructField("event_id",    StringType(),  True),
        StructField("event_time",  StringType(),  True),
        StructField("user_id",     IntegerType(), True),
        StructField("session_id",  StringType(),  True),
        StructField("event_type",  StringType(),  True),
        StructField("product_id",  IntegerType(), True),
        StructField("category",    StringType(),  True),
        StructField("device_type", StringType(),  True),
        StructField("country",     StringType(),  True),
        StructField("price",       DoubleType(),  True),
        StructField("quantity",    IntegerType(), True),
    ])

    # ── 2. Order changes (JSON) ──────────────────────────────────────────────
    order_changes_schema = StructType([
        StructField("order_id",        IntegerType(), True),
        StructField("user_id",         IntegerType(), True),
        StructField("product_id",      IntegerType(), True),
        StructField("quantity",        IntegerType(), True),
        StructField("total_amount",    DoubleType(),  True),
        StructField("order_status",    StringType(),  True),
        StructField("payment_status",  StringType(),  True),
        StructField("order_placed_at", StringType(),  True),
    ])

    # ── 3. Shipment data (JSON) ──────────────────────────────────────────────
    shipment_schema = StructType([
        StructField("order_id",               IntegerType(), True),
        StructField("carrier",                StringType(),  True),
        StructField("tracking_number",        StringType(),  True),
        StructField("status",                 StringType(),  True),
        StructField("status_last_updated_at", StringType(),  True),
    ])

    # ── Start all 4 streams ──────────────────────────────────────────────────
    q1 = ingest_kafka_to_iceberg(spark, "clickstream_events", "clickstream",   clickstream_schema)
    q2 = ingest_server_logs_to_iceberg(spark)
    q3 = ingest_kafka_to_iceberg(spark, "order_changes",      "order_changes", order_changes_schema)
    q4 = ingest_kafka_to_iceberg(spark, "shipment_data",      "shipment_data", shipment_schema)

    logger.info(
        "All 4 Bronze streaming queries running. "
        "Press Ctrl+C to stop. Waiting for any stream to terminate..."
    )
    spark.streams.awaitAnyTermination()
