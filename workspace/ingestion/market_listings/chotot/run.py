import time

from common.io.kafka_producer import create_kafka_producer
from common.utils.logger import get_logger
from ingestion.market_listings.chotot.client import get_current_ads, get_detail
from ingestion.market_listings.chotot.config import (
    BATCH_SIZE,
    KAFKA_TOPIC,
    LOOP_SLEEP_SECONDS,
    MODE,
)
from ingestion.market_listings.chotot.parser import build_raw_record

logger = get_logger(__name__)


def send_record(producer, topic: str, record: dict) -> None:
    future = producer.send(topic, value=record)
    metadata = future.get(timeout=10)
    logger.info(
        "Sent topic=%s partition=%s offset=%s",
        metadata.topic,
        metadata.partition,
        metadata.offset,
    )


def send_mock() -> None:
    producer = create_kafka_producer()
    record = {
        "source": "chotot",
        "domain": "market_listings",
        "entity_type": "mock",
        "record_id": "mock-1",
        "crawled_at_ms": int(time.time() * 1000),
        "payload": {"message": "hello from chotot crawler"},
    }
    send_record(producer, KAFKA_TOPIC, record)
    producer.flush()
    logger.info("Mock message sent.")


def crawl_once() -> int:
    producer = create_kafka_producer()
    ads = get_current_ads(BATCH_SIZE)
    sent = 0

    for ad in ads:
        list_id = ad.get("list_id")
        if not list_id:
            continue

        detail = get_detail(list_id)
        if not detail:
            continue

        record = build_raw_record(ad, detail)
        send_record(producer, KAFKA_TOPIC, record)
        sent += 1
        time.sleep(0.3)

    producer.flush()
    logger.info("Crawl once completed. Sent=%s", sent)
    return sent


def run_loop() -> None:
    while True:
        try:
            crawl_once()
        except Exception as e:
            logger.exception("Loop crawl error: %s", e)

        logger.info("Sleep %s seconds", LOOP_SLEEP_SECONDS)
        time.sleep(LOOP_SLEEP_SECONDS)


if __name__ == "__main__":
    if MODE == "mock":
        send_mock()
    elif MODE == "loop":
        run_loop()
    else:
        crawl_once()