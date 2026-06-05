import os 
from dotenv import load_dotenv

load_dotenv()

APP_ENV = os.getenv("APP_ENV" , "local")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_CLICKSTREAM_TOPIC = os.getenv("KAFKA_CLICKSTREAM_TOPIC" , "clickstream_events")
KAFKA_SERVER_LOGS_TOPIC = os.getenv("KAFKA_SERVER_LOGS_TOPIC", "server_logs")

DATABASE_URL = os.getenv("DATABASE_URL")

MOCK_SHIPPING_API_HOST = os.getenv("MOCK_SHIPPING_API_HOST", "0.0.0.0")
MOCK_SHIPPING_API_PORT = int(os.getenv("MOCK_SHIPPING_API_PORT", "8000"))