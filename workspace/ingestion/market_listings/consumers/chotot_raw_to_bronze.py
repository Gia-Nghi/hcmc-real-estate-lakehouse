import os
from datetime import datetime, timezone

from common.io.kafka_consumer import create_kafka_consumer
from common.io.minio_client import create_minio_client, ensure_bucket, upload_jsonl_lines
from common.utils.logger import get_logger

logger = get_logger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "chotot_raw")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "chotot-raw-to-bronze")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
FLUSH_EVERY = int(os.getenv("FLUSH_EVERY", "20"))


def build_object_name(now: datetime) -> str:
    dt = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    ts = now.strftime("%Y%m%dT%H%M%SZ")
    return (
        f"bronze/market_listings/chotot/"
        f"dt={dt}/hour={hour}/batch_{ts}.jsonl"
    )


def main() -> None:
    consumer = create_kafka_consumer(KAFKA_TOPIC, KAFKA_GROUP_ID)
    minio_client = create_minio_client()
    ensure_bucket(minio_client, MINIO_BUCKET)

    buffer: list[dict] = []

    logger.info("Started consumer topic=%s bucket=%s", KAFKA_TOPIC, MINIO_BUCKET)

    for msg in consumer:
        buffer.append(msg.value)

        if len(buffer) >= FLUSH_EVERY:
            now = datetime.now(timezone.utc)
            object_name = build_object_name(now)
            upload_jsonl_lines(minio_client, MINIO_BUCKET, object_name, buffer)
            logger.info("Uploaded %s records to s3://%s/%s", len(buffer), MINIO_BUCKET, object_name)
            buffer = []


if __name__ == "__main__":
    main()