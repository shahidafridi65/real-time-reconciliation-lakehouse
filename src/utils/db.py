from sqlalchemy import create_engine
import warnings

from config.settings import DATABASE_URL

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is missing. Please set it in your .env file.")

if DATABASE_URL.startswith("postgres://"):
    warnings.warn(
        "DATABASE_URL uses deprecated scheme 'postgres://'. replacing with 'postgresql://' "
        "for SQLAlchemy compatibility.",
        UserWarning,
    )
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # helps validate stale connections before use
)