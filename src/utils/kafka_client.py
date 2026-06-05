import json
from kafka import KafkaProducer
from config.settings import KAFKA_BOOTSTRAP_SERVERS

def build_json_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )


def build_text_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: value.encode("utf-8")
    )