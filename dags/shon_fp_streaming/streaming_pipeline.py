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
    DoubleType,
)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MYSQL_HOST = "217.61.57.46"
MYSQL_PORT = "3306"
MYSQL_DB = "olympic_dataset"
MYSQL_USER = "YOUR_USERNAME"
MYSQL_PASSWORD = "YOUR_PASSWORD"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SOURCE_TOPIC = "athlete_event_results"
OUTPUT_TOPIC = "athlete_avg_stats"

OUTPUT_TABLE = "shon_athlete_avg_stats"

MYSQL_JAR = os.path.join(BASE_DIR, "mysql-connector-j-8.0.32.jar")


def main():
    spark = (
        SparkSession.builder
        .appName("StreamingAthleteAvgStats")
        .config("spark.jars", MYSQL_JAR)
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

    # Етап 2: фільтрація height і weight — залишаємо тільки числові непорожні значення
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

    # Схема JSON-повідомлень з Kafka topic athlete_event_results
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
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Етап 3: JSON value -> dataframe columns
    athlete_event_results_stream = (
        kafka_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), event_schema).alias("data"))
        .select("data.*")
    )

    # Етап 4: join stream athlete_event_results з batch athlete_bio по athlete_id
    enriched_stream = (
        athlete_event_results_stream.alias("r")
        .join(
            athlete_bio_cleaned.alias("b"),
            col("r.athlete_id") == col("b.athlete_id"),
            "inner"
        )
    )

    # Етап 5: розрахунок середнього height і weight по sport, medal, sex, country_noc
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

        # Етап 6.а): запис агрегованого результату у вихідний Kafka topic
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

        (
            kafka_output_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", OUTPUT_TOPIC)
            .save()
        )

        # Етап 6.b): запис агрегованого результату у MySQL таблицю
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
        .outputMode("update")
        .option("checkpointLocation", os.path.join(BASE_DIR, "checkpoint", "athlete_avg_stats"))
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()