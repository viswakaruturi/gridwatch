import argparse
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from fetch_eia import fetch_incremental
from kafka_producer import API_KEY, KAFKA_TOPIC, create_producer, publish_records


STATE_FILE = Path(__file__).resolve().parents[1] / ".state" / "backfill_progress.json"

logger = logging.getLogger(__name__)


def parse_arguments():
    """Read the requested historical date range."""

    parser = argparse.ArgumentParser(
        description="Backfill historical EIA electricity data into Kafka"
    )

    parser.add_argument("--start", required=True, help="Start date: YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date: YYYY-MM-DD")
    parser.add_argument("--force", action="store_true", help="Republish completed dates")

    return parser.parse_args()


def load_progress():
    """Load dates previously published to Kafka."""

    if not STATE_FILE.exists():
        return {"published_days": {}}

    with STATE_FILE.open("r", encoding="utf-8") as file:
        return json.load(file)


def save_progress(progress):
    """Save publication progress after Kafka delivery succeeds."""

    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    temporary_file = STATE_FILE.with_suffix(".tmp")

    with temporary_file.open("w", encoding="utf-8") as file:
        json.dump(progress, file, indent=2)

    temporary_file.replace(STATE_FILE)


def generate_dates(start_date, end_date):
    """Generate every UTC date in the requested range."""

    current_date = start_date

    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def backfill_day(day, producer):
    """Fetch and publish one day of electricity records."""

    start = day.strftime("%Y-%m-%dT00")
    end = day.strftime("%Y-%m-%dT23")

    logger.info("Fetching %s through %s UTC", start, end)

    records = fetch_incremental(API_KEY, start, end)

    if not records:
        raise RuntimeError(f"No records returned for {day.isoformat()}")

    delivered = publish_records(producer, records)

    logger.info(
        "Published %s electricity records for %s",
        delivered,
        day.isoformat()
    )

    return delivered


def main():
    args = parse_arguments()

    if not API_KEY:
        raise ValueError("EIA_API_KEY not found in .env")

    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)

    if start_date > end_date:
        raise ValueError("Start date must be on or before end date")

    if end_date >= datetime.now(timezone.utc).date():
        raise ValueError("Backfill only completed UTC dates")

    progress = load_progress()
    completed_days = progress["published_days"]

    producer = create_producer()

    total_published = 0
    newly_completed = 0

    for day in generate_dates(start_date, end_date):
        day_string = day.isoformat()

        if day_string in completed_days and not args.force:
            logger.info("Skipping already published date: %s", day_string)
            continue

        delivered = backfill_day(day, producer)

        completed_days[day_string] = {
            "records": delivered,
            "topic": KAFKA_TOPIC,
            "published_at": datetime.now(timezone.utc).isoformat()
        }

        save_progress(progress)

        total_published += delivered
        newly_completed += 1

    logger.info(
        "Backfill publication finished: %s dates, %s records delivered",
        newly_completed,
        total_published
    )


if __name__ == "__main__":
    main()
