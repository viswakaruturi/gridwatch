
import os
from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("GridWatch-Postgres-Test")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        host = os.environ["POSTGRES_HOST"]
        port = os.environ["POSTGRES_PORT"]
        database = os.environ["POSTGRES_DB"]
        username = os.environ["POSTGRES_USER"]
        password = os.environ["POSTGRES_PASSWORD"]

        jdbc_url = (
            f"jdbc:postgresql://{host}:{port}/{database}"
        )

        result = (
            spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option(
                "dbtable",
                "(SELECT COUNT(*) AS row_count "
                "FROM electricity) AS counts"
            )
            .option("user", username)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        print("PostgreSQL connection successful!")
        result.show()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
