
import os
import json
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from confluent_kafka import Producer
from dotenv import load_dotenv

from fetch_eia import fetch_incremental


load_dotenv()

API_KEY = os.getenv("EIA_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC = "electricity.raw"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def create_producer():
    """Create a Kafka producer with reliable delivery settings."""

    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "gridwatch-producer",
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
    })


def serialize_record(record):
    """Convert a validated electricity record to JSON bytes."""

    def convert_decimal(obj):
        if isinstance(obj, Decimal):
            return str(obj)
        raise TypeError(f"Cannot serialize type: {type(obj)}")

    return json.dumps(
        record,
        default=convert_decimal
    ).encode("utf-8")


def publish_records(producer, records):
    """Publish records to Kafka and confirm delivery."""

    delivered = 0
    failed = 0

    def delivery_report(err, msg):
        nonlocal delivered, failed

        if err is not None:
            failed += 1
            logger.error("Delivery failed: %s", err)
        else:
            delivered += 1

    for record in records:
        key = record["respondent"].encode("utf-8")
        value = serialize_record(record)

        while True:
            try:
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=key,
                    value=value,
                    callback=delivery_report
                )
                break

            except BufferError:
                logger.warning(
                    "Producer queue full; waiting for delivery"
                )
                producer.poll(1)

        # Trigger delivery callbacks regularly.
        producer.poll(0)

    # Wait for all queued messages to be delivered.
    remaining = producer.flush(timeout=60)

    if remaining > 0:
        raise RuntimeError(
            f"{remaining} Kafka messages were not delivered before timeout"
        )

    logger.info(
        "Kafka publishing complete: %s delivered, %s failed",
        delivered,
        failed
    )

    if failed > 0 or delivered != len(records):
        raise RuntimeError(
            f"Kafka delivery incomplete: "
            f"{delivered}/{len(records)} records delivered"
        )

    return delivered


def main():
    """Fetch one complete UTC hour and publish it to Kafka."""

    if not API_KEY:
        raise ValueError("EIA_API_KEY not found in .env")

    # Use a completed hour from two days ago for the initial test.
    test_hour = (
        datetime.now(timezone.utc) - timedelta(days=2)
    ).replace(minute=0, second=0, microsecond=0)

    start = test_hour.strftime("%Y-%m-%dT%H")
    end = start

    logger.info("Fetching electricity data for %s UTC", start)

    records = fetch_incremental(
        api_key=API_KEY,
        start=start,
        end=end
    )

    logger.info("Fetched %s valid records", len(records))

    if not records:
        logger.warning("No records found; nothing to publish")
        return

    producer = create_producer()
    delivered = publish_records(producer, records)

    logger.info(
        "Successfully published %s electricity records "
        "to Kafka topic '%s'",
        delivered,
        KAFKA_TOPIC
    )


if __name__ == "__main__":
    main()
