import json
import os
from kafka import KafkaProducer


def create_kafka_producer() -> KafkaProducer:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        retries=3,
    )