import json
import os
from kafka import KafkaConsumer


def create_kafka_consumer(topic: str, group_id: str) -> KafkaConsumer:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers.split(","),
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=group_id,
    )