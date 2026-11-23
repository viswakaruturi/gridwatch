
import shutil
import subprocess
import sys
from datetime import date, timedelta

import pendulum
from airflow.sdk import dag, task


SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,"
    "org.postgresql:postgresql:42.7.4"
)


@dag(
    dag_id="gridwatch_pipeline",
    description="GridWatch daily electricity pipeline with automatic recovery",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2026, 11, 18, tz="America/Phoenix"),
    catchup=False,
    max_active_runs=1,
    tags=["gridwatch", "data-engineering"],
)
def gridwatch_pipeline():

    @task
    def calculate_ingestion_window():
        sys.path.insert(0, "/opt/gridwatch/ingestion")

        from run_incremental import LOOKBACK_DAYS, load_checkpoint

        now = pendulum.now("America/Phoenix")

        if now.hour >= 19:
            target_date = now.date()
        else:
            target_date = now.date() - timedelta(days=1)

        previous_date = load_checkpoint()

        normal_start = target_date - timedelta(days=LOOKBACK_DAYS - 1)

        if previous_date is None:
            start_date = normal_start
        else:
            recovery_start = previous_date + timedelta(days=1)
            start_date = min(normal_start, recovery_start)

        needs_processing = (
            previous_date is None or previous_date < target_date
        )

        print(f"Arizona time: {now}", flush=True)
        print(f"Previous checkpoint: {previous_date}", flush=True)
        print(f"Target UTC date: {target_date}", flush=True)
        print(f"Processing required: {needs_processing}", flush=True)

        return {
            "start_date": start_date.isoformat(),
            "end_date": target_date.isoformat(),
            "needs_processing": needs_processing,
        }

    @task.short_circuit
    def check_if_processing_needed(window):
        if not window["needs_processing"]:
            print(
                "GridWatch is up to date. Skipping this interval.",
                flush=True,
            )
            return False

        print(
            f"Processing {window['start_date']} "
            f"through {window['end_date']}",
            flush=True,
        )

        return True


    @task(retries=2, retry_delay=timedelta(minutes=5))
    def check_services_ready():
        import os

        import psycopg2
        from confluent_kafka.admin import AdminClient

        kafka_address = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

        admin = AdminClient({
            "bootstrap.servers": kafka_address,
        })

        metadata = admin.list_topics(timeout=10)

        if not metadata.brokers:
            raise RuntimeError("No Kafka brokers available")

        topic = metadata.topics.get("electricity.raw")

        if topic is None or topic.error is not None:
            raise RuntimeError("Kafka topic electricity.raw is unavailable")

        print(
            f"Kafka ready: {len(metadata.brokers)} broker(s)",
            flush=True,
        )

        connection = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            connect_timeout=10,
        )

        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()

                if result[0] != 1:
                    raise RuntimeError("PostgreSQL readiness check failed")

            print("PostgreSQL ready", flush=True)

        finally:
            connection.close()

        print("All GridWatch dependencies are ready", flush=True)


    @task(retries=1, retry_delay=timedelta(minutes=5))
    def fetch_and_publish_eia(window):
        command = [
            sys.executable,
            "/opt/gridwatch/ingestion/backfill.py",
            "--start", window["start_date"],
            "--end", window["end_date"],
            "--force",
        ]

        subprocess.run(
            command,
            cwd="/opt/gridwatch",
            check=True,
        )

        print("EIA data published to Kafka", flush=True)

    @task(retries=0)
    def process_spark_stream():
        spark_submit = shutil.which("spark-submit")

        if spark_submit is None:
            raise RuntimeError("spark-submit was not found")

        command = [
            spark_submit,
            "--master", "local[2]",
            "--driver-memory", "1g",
            "--packages", SPARK_PACKAGES,
            "/opt/spark/work-dir/process_stream.py",
        ]

        subprocess.run(
            command,
            cwd="/opt/spark/work-dir",
            check=True,
        )

        print("Spark processing completed", flush=True)

    @task(retries=1, retry_delay=timedelta(minutes=2))
    def validate_data_quality(window):
        import os
        import psycopg2

        end_date = window["end_date"]

        connection = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        type,
                        COUNT(*) AS record_count,
                        MAX(period) AS latest_period
                    FROM electricity
                    WHERE period >= %s::date
                      AND period < (%s::date + INTERVAL '1 day')
                    GROUP BY type
                    ORDER BY type
                    """,
                    (end_date, end_date),
                )

                results = cursor.fetchall()

                for measurement_type, count, latest in results:
                    print(
                        f"{measurement_type}: "
                        f"{count} records, latest={latest}",
                        flush=True,
                    )

                demand_count = next(
                    (
                        count
                        for kind, count, _ in results
                        if kind == "D"
                    ),
                    0,
                )

                if demand_count == 0:
                    raise RuntimeError(
                        f"No demand records found for {end_date}"
                    )

                print(
                    f"Data-quality validation passed for {end_date}",
                    flush=True,
                )

        finally:
            connection.close()

    @task
    def save_successful_checkpoint(window):
        sys.path.insert(0, "/opt/gridwatch/ingestion")

        from run_incremental import load_checkpoint, save_checkpoint

        completed_date = date.fromisoformat(window["end_date"])
        previous_date = load_checkpoint()

        if previous_date is None or completed_date > previous_date:
            save_checkpoint(completed_date)
            print(
                f"Checkpoint updated to {completed_date}",
                flush=True,
            )
        else:
            print(
                f"Checkpoint already at {previous_date}",
                flush=True,
            )


    window = calculate_ingestion_window()
    should_process = check_if_processing_needed(window)
    services = check_services_ready()
    ingestion = fetch_and_publish_eia(window)
    spark = process_spark_stream()
    quality = validate_data_quality(window)
    checkpoint = save_successful_checkpoint(window)

    should_process >> services >> ingestion >> spark >> quality >> checkpoint



gridwatch_pipeline()
