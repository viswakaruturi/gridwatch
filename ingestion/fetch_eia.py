
import os
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv("EIA_API_KEY")

BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
PAGE_SIZE = 5000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def create_session():
    """Create an HTTP session with automatic retries."""

    session = requests.Session()

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session.mount("https://", adapter)

    return session


def fetch_eia_page(
    api_key,
    start,
    end,
    offset=0,
    limit=PAGE_SIZE,
    session=None
):
    """Fetch one page of hourly electricity records."""

    params = {
        "api_key": api_key,
        "frequency": "hourly",
        "data[0]": "value",
        "start": start,
        "end": end,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "sort[1][column]": "respondent",
        "sort[1][direction]": "asc",
        "sort[2][column]": "type",
        "sort[2][direction]": "asc",
        "offset": offset,
        "length": limit
    }

    http = session if session is not None else create_session()

    response = http.get(
        BASE_URL,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    result = response.json()

    if "response" not in result or "data" not in result["response"]:
        raise ValueError(f"Unexpected EIA API response: {result}")

    return result["response"]


def fetch_eia_data(api_key, start, end, limit=PAGE_SIZE):
    """Fetch all pages for a specified hourly time window."""

    if not api_key:
        raise ValueError("EIA API key is missing")

    if start > end:
        raise ValueError("Start date must not be after end date")

    all_data = []
    offset = 0

    with create_session() as session:

        while True:
            page = fetch_eia_page(
                api_key=api_key,
                start=start,
                end=end,
                offset=offset,
                limit=limit,
                session=session
            )

            records = page["data"]
            total = int(page["total"])

            all_data.extend(records)
            offset += len(records)

            logger.info(
                "Fetched %s of %s records",
                offset,
                total
            )

            if offset >= total:
                break

            if not records:
                raise RuntimeError(
                    "EIA returned an empty page before all records were fetched"
                )

    return all_data


def validate_records(records):
    """Validate and normalize EIA electricity records."""

    valid_records = []
    invalid_count = 0

    required_fields = ["period", "respondent", "type"]

    for record in records:

        if any(not record.get(field) for field in required_fields):
            invalid_count += 1
            logger.warning(
                "Record missing required fields: %s",
                record
            )
            continue

        normalized = record.copy()

        try:
            value = record.get("value")

            if value is None or value == "":
                normalized["value"] = None
            else:
                numeric_value = Decimal(str(value))

                if not numeric_value.is_finite():
                    raise ValueError("Non-finite numeric value")

                normalized["value"] = numeric_value

            valid_records.append(normalized)

        except (InvalidOperation, ValueError, TypeError):
            invalid_count += 1
            logger.warning(
                "Invalid electricity measurement: %s",
                record
            )

    logger.info(
        "Validation completed: %s valid, %s invalid",
        len(valid_records),
        invalid_count
    )

    return valid_records


def fetch_incremental(api_key, start, end):
    """
    Fetch and validate records for an incremental time window.

    Checkpoint management will be added during Kafka integration.
    """

    logger.info(
        "Starting ingestion for %s to %s",
        start,
        end
    )

    records = fetch_eia_data(api_key, start, end)

    valid_records = validate_records(records)

    return valid_records


if __name__ == "__main__":

    if not API_KEY:
        raise ValueError(
            "EIA_API_KEY not found in .env file"
        )

    # Test using the previous two complete UTC days.
    # Excludes the current incomplete day.
    today = datetime.now(timezone.utc).date()

    start_date = today - timedelta(days=2)
    end_date = today - timedelta(days=1)

    start = f"{start_date.isoformat()}T00"
    end = f"{end_date.isoformat()}T23"

    records = fetch_incremental(
        api_key=API_KEY,
        start=start,
        end=end
    )

    print(f"\nSuccessfully fetched {len(records)} valid records.\n")

    print("Sample records:")

    for record in records[:5]:
        print(record)

    print("\nRegions:", len(set(r["respondent"] for r in records)))

    print(
        "Measurement types:",
        sorted(set(r["type"] for r in records))
    )
