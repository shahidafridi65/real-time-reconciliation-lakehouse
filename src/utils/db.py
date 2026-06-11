from sqlalchemy import create_engine

from config.settings import DATABASE_URL, validate_runtime_config

validate_runtime_config(require_database_url=True)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # helps validate stale connections before use
)