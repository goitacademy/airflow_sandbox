from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp

# Створюємо SparkSession
spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

# Читаємо silver-таблиці
athlete_bio = spark.read.parquet("silver/athlete_bio")
athlete_event_results = spark.read.parquet("silver/athlete_event_results")

# Перейменовуємо country_noc, щоб уникнути дубля
athlete_bio = athlete_bio.withColumnRenamed("country_noc", "bio_country_noc")

# Join по athlete_id
joined = athlete_event_results.join(
    athlete_bio,
    on="athlete_id",
    how="inner"
)

# Агрегація: середня вага та зріст для sport, medal, sex, country
gold = joined.groupBy("sport", "medal", "sex", "bio_country_noc").agg(
    avg("weight").alias("avg_weight"),
    avg("height").alias("avg_height")
).withColumn("timestamp", current_timestamp())

# Запис у gold-layer
gold.write.mode("overwrite").parquet("gold/avg_stats")

# Виводимо результат у логи для Airflow
gold.show(10, truncate=False)

print("✅ Silver_to_Gold job виконано успішно!")

