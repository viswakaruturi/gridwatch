import json
import logging
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "incremental.log"

STATE_DIR = PROJECT_ROOT / ".state"
CHECKPOINT_FILE = STATE_DIR / "incremental_checkpoint.json"

LOOKBACK_DAYS = 3

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,"
    "org.postgresql:postgresql:42.7.4"
)

LOG_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8")
    ]
)

logger = logging.getLogger(__name__)


def load_checkpoint():
    """Read the last successfully processed UTC date."""

    if not CHECKPOINT_FILE.exists():
        logger.info("No incremental checkpoint found")
        return None

    with CHECKPOINT_FILE.open("r", encoding="utf-8") as file:
        checkpoint = json.load(file)

    completed_date = date.fromisoformat(
        checkpoint["last_successful_date"]
    )

    logger.info(
        "Last successful UTC date: %s",
        completed_date
    )

    return completed_date


def save_checkpoint(completed_date):
    """Atomically save the last successfully processed UTC date."""

    checkpoint = {
        "last_successful_date": completed_date.isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

    temporary_file = CHECKPOINT_FILE.with_suffix(".tmp")

    with temporary_file.open("w", encoding="utf-8") as file:
        json.dump(checkpoint, file, indent=2)

    temporary_file.replace(CHECKPOINT_FILE)

    logger.info(
        "Incremental checkpoint saved: %s",
        completed_date
    )


def get_date_range():
    """Determine the UTC date range, including missed-run recovery."""

    today = datetime.now(timezone.utc).date()

    end_date = today - timedelta(days=1)

    normal_start = end_date - timedelta(
        days=LOOKBACK_DAYS - 1
    )

    last_successful_date = load_checkpoint()

    if last_successful_date is None:
        start_date = normal_start
    else:
        recovery_start = last_successful_date + timedelta(days=1)

        start_date = min(normal_start, recovery_start)

    logger.info(
        "Normal lookback starts: %s",
        normal_start
    )

    logger.info(
        "Selected ingestion range: %s to %s",
        start_date,
        end_date
    )

    return start_date.isoformat(), end_date.isoformat()


def run_command(command):
    """Execute a command and capture its output in the log."""

    logger.info("Running: %s", " ".join(command))

    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        bufsize=1
    )

    for line in process.stdout:
        logger.info("%s", line.rstrip())

    return_code = process.wait()

    if return_code != 0:
        raise RuntimeError(
            f"Command failed with exit code {return_code}"
        )


def log_data_quality():
    """Log PostgreSQL measurement coverage and freshness."""

    logger.info("Starting GridWatch data-quality monitoring")

    query = """
        SELECT
            type,
            COUNT(*) AS record_count,
            MAX(period) AS latest_period,
            ROUND(
                (
                    EXTRACT(
                        EPOCH FROM (
                            MAX(MAX(period)) OVER () - MAX(period)
                        )
                    ) / 3600.0
                )::numeric,
                2
            ) AS lag_hours
        FROM electricity
        WHERE type IN ('D', 'DF', 'NG', 'TI')
        GROUP BY type
        ORDER BY type;
    """

    run_command([
        "docker", "compose", "exec", "-T",
        "db",
        "psql",
        "-v", "ON_ERROR_STOP=1",
        "-U", "myuser",
        "-d", "mydatabase",
        "-c", query
    ])

    logger.info("GridWatch data-quality monitoring completed")


def main():
    """Run incremental ingestion with missed-run recovery."""

    logger.info("Starting GridWatch incremental pipeline")

    try:
        start_date, end_date = get_date_range()

        # Step 1: Start Docker services
        run_command([
            "docker", "compose", "up", "-d"
        ])

        # Step 2: Fetch recent and previously missed data
        run_command([
            sys.executable,
            "ingestion/backfill.py",
            "--start", start_date,
            "--end", end_date,
            "--force"
        ])

        # Step 3: Process records with Spark
        run_command([
            "docker", "compose", "exec", "-T",
            "spark",
            "/opt/spark/bin/spark-submit",
            "--master", "local[2]",
            "--packages", SPARK_PACKAGES,
            "/opt/spark/work-dir/process_stream.py"
        ])

        # Step 4: Log data freshness
        log_data_quality()

        # Step 5: Save progress only after successful processing
        completed_date = date.fromisoformat(end_date)

        previous_date = load_checkpoint()

        if previous_date is None or completed_date > previous_date:
            save_checkpoint(completed_date)

        logger.info(
            "GridWatch incremental pipeline completed successfully"
        )

    except Exception:
        logger.exception("GridWatch incremental pipeline failed")
        raise


if __name__ == "__main__":
    main()
