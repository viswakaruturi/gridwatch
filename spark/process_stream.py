import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, from_json, to_timestamp,trim, when, row_number)
from pyspark.sql.types import (DecimalType, StructType, StructField, StringType)
from pyspark.sql.window import Window

KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "electricity.raw"

CHECKPOINT_PATH = "/opt/spark/checkpoints/electricity_validated"

VALID_TYPES = ["D", "DF", "NG", "TI"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def create_spark_session():
    """Initialize Spark using UTC timestamps."""

    return (
        SparkSession.builder
        .appName("GridWatch-Streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def get_electricity_schema():
    """Define the structure of incoming EIA records."""

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
    """Subscribe to electricity messages from Kafka."""

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )


def transform_stream(kafka_df):
    """Parse JSON and convert electricity measurements."""

    schema = get_electricity_schema()

    parsed_df = kafka_df.select(
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("value").cast("string").alias("raw_json")
    ).withColumn(
        "data",
        from_json(col("raw_json"), schema)
    )

    return parsed_df.select(
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
    ).withColumn(
        "period",
        to_timestamp(col("period_raw"), "yyyy-MM-dd'T'HH")
    ).withColumn(
        "respondent",
        trim(col("respondent"))
    ).withColumn(
        "type",
        trim(col("type"))
    ).withColumn(
        "value",
        col("value_raw").cast(DecimalType(18, 3))
    )


def validate_stream(df):
    """Mark invalid records with a descriptive reason."""

    return df.withColumn(
        "validation_error",
        when(
            col("data").isNull(),
            "malformed_json"
        ).when(
            col("period_raw").isNull()
            | (~col("period_raw").rlike(
                r"^\d{4}-\d{2}-\d{2}T\d{2}$"
            ))
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
    """Keep the latest Kafka message for each electricity key."""

    window = (
        Window.partitionBy(
            "period",
            "respondent",
            "type"
        )
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


def process_batch(batch_df, batch_id):
    """Validate and deduplicate one Spark micro-batch."""

    batch_df.persist()

    try:
        total_count = batch_df.count()

        if total_count == 0:
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
        logger.info("Invalid records: %s", invalid_count)
        logger.info("Valid records: %s", valid_count)
        logger.info(
            "Duplicates removed within batch: %s",
            valid_count - output_count
        )
        logger.info("Output records: %s", output_count)

        if invalid_count > 0:
            logger.warning("Sample invalid records:")
            invalid_df.select(
                "raw_json",
                "validation_error"
            ).show(5, truncate=False)

        output_df.show(20, truncate=False)

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
