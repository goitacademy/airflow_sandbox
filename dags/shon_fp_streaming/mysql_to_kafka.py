import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MYSQL_HOST = "217.61.57.46"
MYSQL_PORT = "3306"
MYSQL_DB = "neo_data"
MYSQL_USER = "neo_data_admin"
MYSQL_PASSWORD = "Proyahaxuqithab9oplp"

KAFKA_BOOTSTRAP_SERVERS = "77.81.230.104:9092"
KAFKA_USER = "admin"
KAFKA_PASSWORD = "VawEzo1ikLtrA8Ug8THa"

SOURCE_TOPIC = "athlete_event_results"

MYSQL_JAR = os.path.join(BASE_DIR, "mysql-connector-j-8.0.32.jar")


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
    if not KAFKA_PASSWORD:
        raise Exception("KAFKA_PASSWORD environment variable is required")

    spark = (
        SparkSession.builder
        .appName("MySQLToKafkaAthleteEventResults")
        .config("spark.jars", MYSQL_JAR)
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