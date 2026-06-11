import os

from dotenv import load_dotenv

load_dotenv()


def normalize_database_url(database_url: str | None) -> str | None:
    """Normalize the database URL for SQLAlchemy compatibility."""
    if not database_url:
        return database_url

    if database_url.startswith("postgres://"):
        return database_url.replace("postgres://", "postgresql://", 1)

    return database_url


def validate_runtime_config(*, require_database_url: bool = True) -> None:
    """Validate the minimum environment values needed for local execution."""
    database_url = normalize_database_url(os.getenv("DATABASE_URL"))
    kafka_bootstrap_servers = (
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") or "localhost:9092"
    ).strip()

    missing = []

    if require_database_url and not database_url:
        missing.append("DATABASE_URL")

    if not kafka_bootstrap_servers:
        missing.append("KAFKA_BOOTSTRAP_SERVERS")

    if missing:
        raise ValueError(
            "Missing required configuration values: " + ", ".join(missing)
        )


APP_ENV = (os.getenv("APP_ENV", "local") or "local").strip().lower()

KAFKA_BOOTSTRAP_SERVERS = (os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") or "localhost:9092").strip()
KAFKA_CLICKSTREAM_TOPIC = (os.getenv("KAFKA_CLICKSTREAM_TOPIC", "clickstream_events") or "clickstream_events").strip()
KAFKA_SERVER_LOGS_TOPIC = (os.getenv("KAFKA_SERVER_LOGS_TOPIC", "server_logs") or "server_logs").strip()

DATABASE_URL = normalize_database_url(os.getenv("DATABASE_URL"))

MOCK_SHIPPING_API_HOST = (os.getenv("MOCK_SHIPPING_API_HOST", "0.0.0.0") or "0.0.0.0").strip()
MOCK_SHIPPING_API_PORT = int(os.getenv("MOCK_SHIPPING_API_PORT", "8000") or 8000)