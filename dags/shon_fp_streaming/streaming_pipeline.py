import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    avg,
    current_timestamp,
    when,
    to_json,
    struct,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_USER = os.getenv("KAFKA_USER")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")

SOURCE_TOPIC = "athlete_event_results"
OUTPUT_TOPIC = "athlete_avg_stats_shon"

OUTPUT_TABLE = "athlete_avg_stats_shon"
CHECKPOINT_LOCATION = "/tmp/shon_fp_streaming_checkpoint"


def validate_env():
    required_vars = [
        "MYSQL_HOST",
        "MYSQL_PORT",
        "MYSQL_DB",
        "MYSQL_USER",
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_USER",
        "KAFKA_PASSWORD",
    ]

    missing_vars = [
        var_name
        for var_name in required_vars
        if not os.getenv(var_name)
    ]

    if missing_vars:
        raise Exception(f"Missing environment variables: {missing_vars}")


def kafka_security_options(reader_or_writer):
    return (
        reader_or_writer
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";'
        )
    )


def main():
    validate_env()

    spark = (
        SparkSession.builder
        .appName("ShonStreamingAthleteAvgStats")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

    # Етап 1: зчитування фізичних показників атлетів з MySQL таблиці athlete_bio
    athlete_bio = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "athlete_bio")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .load()
    )

    # Етап 2: фільтрація height та weight — залишаємо тільки числові значення
    athlete_bio_cleaned = (
        athlete_bio
        .withColumn("height_num", col("height").cast("double"))
        .withColumn("weight_num", col("weight").cast("double"))
        .filter(col("height_num").isNotNull())
        .filter(col("weight_num").isNotNull())
        .select(
            col("athlete_id").cast("integer").alias("athlete_id"),
            col("sex"),
            col("country_noc"),
            col("height_num"),
            col("weight_num"),
        )
    )

    event_schema = StructType([
        StructField("edition", StringType(), True),
        StructField("edition_id", IntegerType(), True),
        StructField("country_noc", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("event", StringType(), True),
        StructField("result_id", IntegerType(), True),
        StructField("athlete", StringType(), True),
        StructField("athlete_id", IntegerType(), True),
        StructField("pos", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("isTeamSport", StringType(), True),
    ])

    # Етап 3: зчитування результатів змагань з Kafka topic athlete_event_results
    kafka_stream = (
        kafka_security_options(
            spark.readStream
            .format("kafka")
            .option("subscribe", SOURCE_TOPIC)
            .option("startingOffsets", "earliest")
        )
        .load()
    )

    # Етап 3: переведення Kafka JSON value у DataFrame-формат
    athlete_event_results_stream = (
        kafka_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), event_schema).alias("data"))
        .select("data.*")
        .filter(col("athlete_id").isNotNull())
    )

    # Етап 4: об'єднання Kafka stream з athlete_bio по athlete_id
    enriched_stream = (
        athlete_event_results_stream.alias("r")
        .join(
            athlete_bio_cleaned.alias("b"),
            col("r.athlete_id") == col("b.athlete_id"),
            "inner"
        )
    )

    # Етап 5: середній зріст і вага по sport, medal, sex, country_noc
    avg_stats_stream = (
        enriched_stream
        .withColumn(
            "medal_clean",
            when(
                col("r.medal").isNull() | (col("r.medal") == ""),
                "No medal"
            ).otherwise(col("r.medal"))
        )
        .groupBy(
            col("r.sport").alias("sport"),
            col("medal_clean").alias("medal"),
            col("b.sex").alias("sex"),
            col("b.country_noc").alias("country_noc")
        )
        .agg(
            avg(col("b.height_num")).alias("avg_height"),
            avg(col("b.weight_num")).alias("avg_weight")
        )
        .withColumn("timestamp", current_timestamp())
    )

    def write_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        kafka_output_df = batch_df.select(
            to_json(
                struct(
                    col("sport"),
                    col("medal"),
                    col("sex"),
                    col("country_noc"),
                    col("avg_height"),
                    col("avg_weight"),
                    col("timestamp"),
                )
            ).alias("value")
        )

        # Етап 6.а): запис у вихідний Kafka topic
        (
            kafka_security_options(
                kafka_output_df.write
                .format("kafka")
                .option("topic", OUTPUT_TOPIC)
            )
            .save()
        )

        # Етап 6.b): запис у MySQL таблицю
        (
            batch_df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", OUTPUT_TABLE)
            .option("user", MYSQL_USER)
            .option("password", MYSQL_PASSWORD)
            .mode("append")
            .save()
        )

    # Етап 6: foreachBatch fan-out у Kafka та MySQL
    query = (
        avg_stats_stream
        .writeStream
        .foreachBatch(write_batch)
        .outputMode("complete")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()