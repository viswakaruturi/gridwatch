import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, trim, when, row_number
from pyspark.sql.types import DecimalType, StructType, StructField, StringType
from pyspark.sql.window import Window


KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "electricity.raw"

CHECKPOINT_PATH = "/opt/spark/checkpoints/electricity_postgres_v1"

VALID_TYPES = ["D", "DF", "NG", "TI"]

POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def create_spark_session():
    """Create a Spark session using UTC timestamps."""

    return (
        SparkSession.builder
        .appName("GridWatch-Streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def get_electricity_schema():
    """Define the schema of EIA electricity messages."""

    return StructType([
        StructField("period", StringType(), True),
        StructField("respondent", StringType(), True),
        StructField("respondent-name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("type-name", StringType(), True),
        StructField("value", StringType(), True),
        StructField("value-units", StringType(), True)
    ])


def read_kafka_stream(spark):
    """Read electricity records from Kafka."""

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )


def transform_stream(kafka_df):
    """Parse JSON and normalize electricity fields."""

    schema = get_electricity_schema()

    parsed_df = (
        kafka_df.select(
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("value").cast("string").alias("raw_json")
        )
        .withColumn("data", from_json(col("raw_json"), schema))
    )

    return (
        parsed_df.select(
            "kafka_partition",
            "kafka_offset",
            "raw_json",
            col("data"),
            col("data.period").alias("period_raw"),
            col("data.respondent").alias("respondent"),
            col("data.`respondent-name`").alias("respondent_name"),
            col("data.type").alias("type"),
            col("data.`type-name`").alias("type_name"),
            col("data.value").alias("value_raw"),
            col("data.`value-units`").alias("value_units")
        )
        .withColumn(
            "period",
            to_timestamp(col("period_raw"), "yyyy-MM-dd'T'HH")
        )
        .withColumn("respondent", trim(col("respondent")))
        .withColumn("type", trim(col("type")))
        .withColumn(
            "value",
            col("value_raw").cast(DecimalType(18, 3))
        )
    )


def validate_stream(df):
    """Identify invalid electricity records."""

    return df.withColumn(
        "validation_error",
        when(
            col("data").isNull(),
            "malformed_json"
        ).when(
            col("period_raw").isNull()
            | (~col("period_raw").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}$"))
            | col("period").isNull(),
            "invalid_period"
        ).when(
            col("respondent").isNull()
            | (col("respondent") == ""),
            "missing_respondent"
        ).when(
            col("type").isNull()
            | (~col("type").isin(VALID_TYPES)),
            "invalid_measurement_type"
        ).when(
            col("value_raw").isNotNull()
            & (trim(col("value_raw")) != "")
            & col("value").isNull(),
            "invalid_numeric_value"
        )
    )


def deduplicate_batch(df):
    """Keep one record per period, respondent, and type."""

    window = (
        Window.partitionBy("period", "respondent", "type")
        .orderBy(
            col("kafka_offset").desc(),
            col("kafka_partition").desc()
        )
    )

    return (
        df.withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


def get_postgres_connection(spark):
    """Create a PostgreSQL JDBC connection."""

    properties = spark._jvm.java.util.Properties()
    properties.setProperty("user", POSTGRES_USER)
    properties.setProperty("password", POSTGRES_PASSWORD)

    connection = spark._jvm.org.postgresql.Driver().connect(
        JDBC_URL, properties
    )

    if connection is None:
        raise RuntimeError("PostgreSQL connection failed")

    return connection


def execute_sql(spark, sql):
    """Execute a PostgreSQL SQL statement."""

    connection = get_postgres_connection(spark)

    try:
        statement = connection.createStatement()

        try:
            statement.execute(sql)
        finally:
            statement.close()

    finally:
        connection.close()


def write_batch_to_postgres(spark, output_df, batch_id):
    """Stage records and merge them into the electricity table."""

    stage_table = f"electricity_stage_{int(batch_id)}"

    execute_sql(spark, f"DROP TABLE IF EXISTS {stage_table}")

    execute_sql(
        spark,
        f"CREATE TABLE {stage_table} (LIKE electricity INCLUDING DEFAULTS)"
    )

    (
        output_df.coalesce(2)
        .write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", stage_table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "1000")
        .mode("append")
        .save()
    )

    upsert_sql = f"""
        INSERT INTO electricity (
            period,
            respondent,
            respondent_name,
            type,
            type_name,
            value,
            value_units
        )
        SELECT
            period,
            respondent,
            respondent_name,
            type,
            type_name,
            value,
            value_units
        FROM {stage_table}
        WHERE TRUE
        ON CONFLICT (period, respondent, type)
        DO UPDATE SET
            respondent_name = EXCLUDED.respondent_name,
            type_name = EXCLUDED.type_name,
            value = EXCLUDED.value,
            value_units = EXCLUDED.value_units
    """

    connection = get_postgres_connection(spark)

    try:
        connection.setAutoCommit(False)
        statement = connection.createStatement()

        try:
            affected_rows = statement.executeUpdate(upsert_sql)
            statement.execute(f"DROP TABLE {stage_table}")
            connection.commit()

            logger.info(
                "Batch %s: PostgreSQL upsert complete. Affected rows: %s",
                batch_id,
                affected_rows
            )

        finally:
            statement.close()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def process_batch(batch_df, batch_id):
    """Validate, deduplicate, and store a Kafka micro-batch."""

    batch_df.persist()

    try:
        total_count = batch_df.count()

        if total_count == 0:
            logger.info("Batch %s: no records", batch_id)
            return

        invalid_df = batch_df.filter(
            col("validation_error").isNotNull()
        )

        valid_df = batch_df.filter(
            col("validation_error").isNull()
        )

        invalid_count = invalid_df.count()
        valid_count = total_count - invalid_count

        cleaned_df = deduplicate_batch(valid_df)

        output_df = cleaned_df.select(
            "period",
            "respondent",
            "respondent_name",
            "type",
            "type_name",
            "value",
            "value_units"
        )

        output_count = output_df.count()

        logger.info("Batch ID: %s", batch_id)
        logger.info("Total records: %s", total_count)
        logger.info("Valid records: %s", valid_count)
        logger.info("Invalid records: %s", invalid_count)
        logger.info(
            "Duplicates removed within batch: %s",
            valid_count - output_count
        )
        logger.info("Output records: %s", output_count)

        if invalid_count > 0:
            invalid_df.select(
                "raw_json",
                "validation_error"
            ).show(5, truncate=False)

        if output_count > 0:
            write_batch_to_postgres(
                batch_df.sparkSession,
                output_df,
                batch_id
            )

            logger.info(
                "Batch %s successfully stored in PostgreSQL",
                batch_id
            )

            output_df.show(10, truncate=False)

    finally:
        batch_df.unpersist()


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        kafka_df = read_kafka_stream(spark)
        transformed_df = transform_stream(kafka_df)
        validated_df = validate_stream(transformed_df)

        query = (
            validated_df.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
