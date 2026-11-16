
import logging
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "incremental.log"

LOOKBACK_DAYS = 3

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,"
    "org.postgresql:postgresql:42.7.4"
)

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8")
    ]
)

logger = logging.getLogger(__name__)


def get_date_range():
    """Get the last three completed UTC dates."""

    today = datetime.now(timezone.utc).date()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=LOOKBACK_DAYS - 1)

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


def main():
    """Fetch recent electricity data and process it with Spark."""

    start_date, end_date = get_date_range()

    logger.info("Starting GridWatch incremental pipeline")
    logger.info("UTC date range: %s to %s", start_date, end_date)

    try:
        run_command([
            "docker", "compose", "up", "-d"
        ])

        run_command([
            sys.executable,
            "ingestion/backfill.py",
            "--start", start_date,
            "--end", end_date,
            "--force"
        ])

        run_command([
            "docker", "compose", "exec", "-T",
            "spark",
            "/opt/spark/bin/spark-submit",
            "--master", "local[2]",
            "--packages", SPARK_PACKAGES,
            "/opt/spark/work-dir/process_stream.py"
        ])

        logger.info("GridWatch incremental pipeline completed successfully")

    except Exception:
        logger.exception("GridWatch incremental pipeline failed")
        raise


if __name__ == "__main__":
    main()
