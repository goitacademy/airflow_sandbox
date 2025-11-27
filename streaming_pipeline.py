# Для запуску файлу виконати команду spark-submit \
#  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
#  --jars mysql-connector-j-8.0.32.jar \
#  streaming_pipeline.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    avg,
    when,
    to_json,
    struct,
    expr,  
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

# --- КРЕДЕНЦІАЛИ  ---
# Налаштування MySQL
MYSQL_HOST = "217.61.57.46"
MYSQL_PORT = "3306"
MYSQL_DB = "olympic_dataset"
MYSQL_USER = "neo_data_admin"
MYSQL_PASSWORD = "Proyahaxuqithab9oplp"
JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
JDBC_JAR = "mysql-connector-j-8.0.32.jar"

# Налаштування Kafka
KAFKA_SERVER = "77.81.230.104:9092"
INPUT_TOPIC = "athlete_event_results"
OUTPUT_TOPIC = "enriched_athlete_stats"
OUTPUT_TABLE = "athlete_enriched_agg_danich"

KAFKA_USERNAME = "admin"
KAFKA_PASSWORD = "VawEzo1ikLtrA8Ug8THa"

KAFKA_JAAS_CONFIG = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
)

# Checkpoint для стримінгу
CHECKPOINT_LOCATION = "/tmp/checkpoints_athlete_enriched_danich"

# Схема для даних, що надходять з Kafka (JSON у полі value)
kafka_schema = StructType(
    [
        StructField("event_id", IntegerType(), True),
        StructField("athlete_id", IntegerType(), True),
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("country_noc", StringType(), True),
    ]
)


def run_streaming_pipeline():
    # --- Створення SparkSession ---
    spark = (
        SparkSession.builder.appName("StreamingPipeline")
        .config("spark.jars", JDBC_JAR)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # --- ЕТАП 1. Читання athlete_bio з MySQL ---
    print(">>> ЕТАП 1: Читання athlete_bio з MySQL...")

    athlete_bio_df = (
        spark.read.format("jdbc")
        .options(
            url=JDBC_URL,
            driver=JDBC_DRIVER,
            dbtable="athlete_bio",
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
        )
        .load()
    )

    # --- ЕТАП 2. Очищення height/weight з try_cast ---
    print(">>> ЕТАП 2: Фільтрація та очищення athlete_bio (через try_cast)...")

    athlete_bio_cleaned = (
        athlete_bio_df
        # try_cast: усі некоректні значення → NULL, без помилки
        .withColumn("height_f", expr("try_cast(height as float)"))
        .withColumn("weight_f", expr("try_cast(weight as float)"))
        .filter(col("height_f").isNotNull() & col("weight_f").isNotNull())
        .select(
            col("athlete_id"),
            col("sex"),
            col("height_f").alias("height"),
            col("weight_f").alias("weight"),
        )
    )

    athlete_bio_cleaned.cache()
    print(
        f"Кількість записів у athlete_bio після очищення: {athlete_bio_cleaned.count()}"
    )

    # --- ЕТАП 3. Читання стриму з Kafka ---
    print(">>> ЕТАП 3: Читання стримінгових даних з Kafka...")

    kafka_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", KAFKA_JAAS_CONFIG)
        .load()
    )

    event_stream_df = (
        kafka_stream_df.selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), kafka_schema).alias("data"))
        .select("data.*")
        .withColumn("athlete_id", col("athlete_id").cast(IntegerType()))
    )

    # --- ЕТАП 4. Join зі статичним DF з MySQL ---
    print(">>> ЕТАП 4: Збагачення (Join) стриму статичними даними...")

    enriched_stream_df = event_stream_df.join(
        athlete_bio_cleaned,
        on="athlete_id",
        how="inner",
    )

    # --- ЕТАП 5. Трансформації + підготовка до агрегації в foreachBatch ---
    print(">>> ЕТАП 5: Підготовка даних до агрегації...")

    agg_df = enriched_stream_df.withColumn(
        "medal",
        when(col("medal").isNull() | (col("medal") == ""), "No Medal").otherwise(
            col("medal")
        ),
    )

    group_cols = ["sport", "medal", "sex", "country_noc"]

    # --- foreachBatch-функція: тут агрегація + запис у Kafka і MySQL ---
    def foreach_batch_function(batch_df, batch_id):
        print(f"\n--- Обробка Мікробатчу {batch_id} ---")

        aggregated_batch_df = (
            batch_df.groupBy(group_cols)
            .agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight"),
            )
            .withColumn("timestamp", current_timestamp())
        )

        print(f"Результат агрегації для батчу {batch_id}:")
        aggregated_batch_df.show(truncate=False)

        # --- 6a. Запис у Kafka ---
        print(">>> ЕТАП 6.a): Запис збагачених даних у Kafka...")

        kafka_output_df = aggregated_batch_df.withColumn(
            "value", to_json(struct("*"))
        ).selectExpr(
            "CAST(sport AS STRING) AS key",
            "CAST(value AS STRING) AS value",
        )

        (
            kafka_output_df.write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVER)
            .option("topic", OUTPUT_TOPIC)
            .option("kafka.security.protocol", "SASL_PLAINTEXT")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", KAFKA_JAAS_CONFIG)
            .mode("append")
            .save()
        )

        print("Запис у Kafka завершено.")

        # --- 6b. Запис у MySQL ---
        print(">>> ЕТАП 6.b): Збереження збагачених даних до MySQL...")

        (
            aggregated_batch_df.write.format("jdbc")
            .option("url", JDBC_URL)
            .option("driver", JDBC_DRIVER)
            .option("dbtable", OUTPUT_TABLE)
            .option("user", MYSQL_USER)
            .option("password", MYSQL_PASSWORD)
            .mode("append")
            .save()
        )

        print("Запис у MySQL завершено.")

    # --- Запуск стриму ---
    print("\n--- СТАРТ СТРИМІНГУ ---")

    query = (
        agg_df.writeStream.foreachBatch(foreach_batch_function)
        .outputMode("append")  # кожен батч додаємо
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    print("Стрімінг запущено. Він працюватиме, поки ти його не зупиниш (Ctrl+C).")
    query.awaitTermination()

    print("Зупинка стримінгу.")
    spark.stop()
    print("✅ Spark зупинено коректно.")


if __name__ == "__main__":
    run_streaming_pipeline()
