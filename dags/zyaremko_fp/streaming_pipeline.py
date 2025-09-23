from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# ===============================
# Етап 1. SparkSession + MySQL
# ===============================
spark = SparkSession.builder \
    .appName("EndToEndStreamingPipeline") \
    .config("spark.jars", "/home/zyaremko/airflow_sandbox/dags/zyaremko_fp/mysql-connector-j-8.0.32.jar") \
    .getOrCreate()

print("✅ Spark сесія створена")

jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

athlete_bio_df = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="athlete_bio",
    user=jdbc_user,
    password=jdbc_password
).load()

print("✅ Етап 1: Біо-дані завантажено")

# ===============================
# Етап 2. Фільтрація біо-даних
# ===============================
athlete_bio_df_clean = athlete_bio_df \
    .filter(col("height").cast("int").isNotNull()) \
    .filter(col("weight").cast("int").isNotNull())

print("✅ Етап 2: Біо-дані відфільтровано")

# ===============================
# Етап 3. Дані з Kafka
# ===============================
kafka_server = "localhost:9092"
input_topic = "athlete_event_results"

event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("athlete_id", StringType()),
    StructField("sport", StringType()),
    StructField("medal", StringType()),
    StructField("year", StringType())
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "latest") \
    .load()

event_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), event_schema).alias("data")) \
    .select("data.*")

print("✅ Етап 3: Дані з Kafka зчитано")

# ===============================
# Етап 4. Join
# ===============================
joined_df = event_df.join(
    athlete_bio_df_clean,
    on="athlete_id",
    how="inner"
)

print("✅ Етап 4: Join виконано")

# ===============================
# Етап 5. Агрегація
# ===============================
aggregated_df = joined_df.groupBy(
    "sport", "medal", "sex", "country_noc"
).agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight")
).withColumn("calculated_at", current_timestamp())

print("✅ Етап 5: Агрегація виконана")

# ===============================
# Етап 6. Sink у Kafka + MySQL
# ===============================
def write_to_sinks(batch_df, batch_id):
    # 6a. Kafka
    batch_df.selectExpr(
        "to_json(named_struct('sport', sport, 'medal', medal, 'sex', sex, "
        "'country_noc', country_noc, 'avg_height', avg_height, "
        "'avg_weight', avg_weight, 'calculated_at', calculated_at)) AS value"
    ).write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("topic", "aggregated_athlete_stats") \
        .save()

    # 6b. MySQL
    batch_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "aggregated_athlete_stats") \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .mode("append") \
        .save()

print("✅ Етап 6: Sink функція визначена")

# ===============================
# Запуск стріму
# ===============================
query = aggregated_df.writeStream \
    .foreachBatch(write_to_sinks) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark_checkpoints") \
    .start()

print("🚀 Потік запущено...")
query.awaitTermination()
