import os
from datetime import timedelta

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values


load_dotenv(".env")

BATCH_SIZE = 1000
LOOKBACK_DAYS = 3


def sync_to_neon():
    local_connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )

    try:
        neon_connection = psycopg2.connect(
            os.environ["NEON_DATABASE_URL"],
            connect_timeout=15,
        )
    except Exception:
        local_connection.close()
        raise

    total_processed = 0

    try:
        with local_connection, neon_connection:
            with neon_connection.cursor() as cursor:
                cursor.execute("SELECT MAX(period) FROM electricity")
                latest_cloud_period = cursor.fetchone()[0]

            if latest_cloud_period is None:
                sync_start = None
                print("Cloud database is empty. Starting full sync.")
            else:
                sync_start = latest_cloud_period - timedelta(
                    days=LOOKBACK_DAYS
                )
                print(f"Synchronizing records from: {sync_start}")

            select_query = """
                SELECT
                    period,
                    respondent,
                    respondent_name,
                    type,
                    type_name,
                    value,
                    value_units
                FROM electricity
            """

            if sync_start is not None:
                select_query += " WHERE period >= %s"

            select_query += " ORDER BY period, respondent, type"

            insert_query = """
                INSERT INTO electricity (
                    period,
                    respondent,
                    respondent_name,
                    type,
                    type_name,
                    value,
                    value_units
                )
                VALUES %s
                ON CONFLICT (period, respondent, type)
                DO UPDATE SET
                    respondent_name = EXCLUDED.respondent_name,
                    type_name = EXCLUDED.type_name,
                    value = EXCLUDED.value,
                    value_units = EXCLUDED.value_units
                WHERE (
                    electricity.respondent_name,
                    electricity.type_name,
                    electricity.value,
                    electricity.value_units
                ) IS DISTINCT FROM (
                    EXCLUDED.respondent_name,
                    EXCLUDED.type_name,
                    EXCLUDED.value,
                    EXCLUDED.value_units
                )
            """

            with local_connection.cursor(
                name="gridwatch_sync_source"
            ) as source_cursor:
                source_cursor.itersize = BATCH_SIZE

                if sync_start is None:
                    source_cursor.execute(select_query)
                else:
                    source_cursor.execute(
                        select_query,
                        (sync_start,),
                    )

                with neon_connection.cursor() as target_cursor:
                    while True:
                        rows = source_cursor.fetchmany(BATCH_SIZE)

                        if not rows:
                            break

                        execute_values(
                            target_cursor,
                            insert_query,
                            rows,
                            page_size=BATCH_SIZE,
                        )

                        total_processed += len(rows)
                        print(
                            f"Processed {total_processed:,} records"
                        )

        print(
            f"Neon synchronization completed: "
            f"{total_processed:,} records examined."
        )

    finally:
        local_connection.close()
        neon_connection.close()


if __name__ == "__main__":
    sync_to_neon()
