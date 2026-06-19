import os
from dotenv import load_dotenv

load_dotenv()


def _clean(value: str | None, default: str = "") -> str:
    return (value if value is not None else default).strip()


def normalize_database_url(database_url: str | None) -> str | None:
    if not database_url:
        return database_url
    if database_url.startswith("postgres://"):
        return database_url.replace("postgres://", "postgresql://", 1)
    return database_url


def validate_runtime_config(*, require_database_url: bool = True) -> None:
    missing = []
    if require_database_url and not DATABASE_URL:
        missing.append("DATABASE_URL")
    if not KAFKA_BOOTSTRAP_SERVERS:
        missing.append("KAFKA_BOOTSTRAP_SERVERS")
    if missing:
        raise ValueError("Missing required configuration values: " + ", ".join(missing))


def validate_redshift_config() -> None:
    missing = [
        name
        for name, value in {
            "REDSHIFT_HOST": REDSHIFT_HOST,
            "REDSHIFT_DATABASE": REDSHIFT_DATABASE,
            "REDSHIFT_USER": REDSHIFT_USER,
            "REDSHIFT_PASSWORD": REDSHIFT_PASSWORD,
        }.items()
        if not value
    ]
    if missing:
        raise ValueError("Missing required Redshift configuration values: " + ", ".join(missing))


APP_ENV = _clean(os.getenv("APP_ENV"), "local").lower()

KAFKA_BOOTSTRAP_SERVERS = _clean(os.getenv("KAFKA_BOOTSTRAP_SERVERS"), "localhost:9092")
KAFKA_CLICKSTREAM_TOPIC = _clean(os.getenv("KAFKA_CLICKSTREAM_TOPIC"), "clickstream_events")
KAFKA_SERVER_LOGS_TOPIC = _clean(os.getenv("KAFKA_SERVER_LOGS_TOPIC"), "server_logs")

DATABASE_URL = normalize_database_url(_clean(os.getenv("DATABASE_URL")) or None)

MOCK_SHIPPING_API_HOST = _clean(os.getenv("MOCK_SHIPPING_API_HOST"), "0.0.0.0")
MOCK_SHIPPING_API_PORT = int(_clean(os.getenv("MOCK_SHIPPING_API_PORT"), "8000"))

AWS_REGION = _clean(os.getenv("AWS_REGION"), "us-east-1")
AWS_S3_BUCKET_NAME = _clean(os.getenv("AWS_S3_BUCKET_NAME"))
AWS_S3_PREFIX = _clean(os.getenv("AWS_S3_PREFIX"), "bronze/raw").strip("/")
AWS_ACCESS_KEY_ID = _clean(os.getenv("AWS_ACCESS_KEY_ID"))
AWS_SECRET_ACCESS_KEY = _clean(os.getenv("AWS_SECRET_ACCESS_KEY"))
AWS_SESSION_TOKEN = _clean(os.getenv("AWS_SESSION_TOKEN"))

REDSHIFT_HOST = _clean(os.getenv("REDSHIFT_HOST"))
REDSHIFT_PORT = int(_clean(os.getenv("REDSHIFT_PORT"), "5439"))
REDSHIFT_DATABASE = _clean(os.getenv("REDSHIFT_DATABASE"), "dev")
REDSHIFT_USER = _clean(os.getenv("REDSHIFT_USER"))
REDSHIFT_PASSWORD = _clean(os.getenv("REDSHIFT_PASSWORD"))
