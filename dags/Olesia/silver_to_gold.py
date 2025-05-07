from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
from pathlib import Path

# Створення Spark-сесії
spark = SparkSession.builder.appName("SilverToGoldLayer").getOrCreate()

# Створення директорії gold
Path("gold").mkdir(parents=True, exist_ok=True)

# Етап 1: Зчитування з silver
df_bio = spark.read.parquet("silver/athlete_bio")
df_results = spark.read.parquet("silver/athlete_event_results")

# Етап 2: Join за athlete_id
df_joined = df_results.join(df_bio, on="athlete_id", how="inner")

# Етап 3: Обчислення середніх значень з урахуванням неоднозначності
df_avg = df_joined.groupBy(
    "sport",
    "medal",
    "sex",
    df_bio["country_noc"]  # ← Вирішує проблему AMBIGUOUS_REFERENCE
).agg(
    avg("weight").alias("avg_weight"),
    avg("height").alias("avg_height")
).withColumn(
    "timestamp", current_timestamp()
)

# Етап 4: Виведення результату
df_avg.show()

# Етап 5: Збереження в gold
df_avg.write.mode("overwrite").parquet("gold/avg_stats")

# Завершення сесії
spark.stop()
