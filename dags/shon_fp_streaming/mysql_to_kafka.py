import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct


MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_USER = os.getenv("KAFKA_USER")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")

SOURCE_TOPIC = "athlete_event_results"


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


def kafka_security_options(writer_or_reader):
    return (
        writer_or_reader
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
        .appName("ShonMySQLToKafkaAthleteEventResults")
        .getOrCreate()
    )

    jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

    # Етап 3.1: зчитування athlete_event_results з MySQL
    athlete_event_results = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "athlete_event_results")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .load()
    )

    # Етап 3.2: перетворення рядків DataFrame у JSON для Kafka
    kafka_df = athlete_event_results.select(
        col("athlete_id").cast("string").alias("key"),
        to_json(
            struct(*[col(column_name) for column_name in athlete_event_results.columns])
        ).alias("value")
    )

    # Етап 3.3: запис у Kafka topic athlete_event_results
    (
        kafka_security_options(
            kafka_df.write
            .format("kafka")
            .option("topic", SOURCE_TOPIC)
        )
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    main()