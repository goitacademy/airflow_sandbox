from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col
from pyspark.sql.types import DoubleType
from pathlib import Path

spark = SparkSession.builder.appName("SilverToGoldLayer").getOrCreate()

Path("gold").mkdir(parents=True, exist_ok=True)

df_bio = spark.read.parquet("silver/athlete_bio")
df_results = spark.read.parquet("silver/athlete_event_results")

# Приведення типів до чисельних (вимога ТЗ)
df_bio = df_bio.withColumn("weight", col("weight").cast(DoubleType())) \
               .withColumn("height", col("height").cast(DoubleType()))

# Об'єднання таблиць (Inner Join)
df_joined = df_results.join(df_bio, on="athlete_id", how="inner")

# Розрахунок середніх показників ваги та росту
df_avg = df_joined.groupBy(
    "sport",
    "medal",
    "sex",
    "country_noc"
).agg(
    avg("weight").alias("avg_weight"),
    avg("height").alias("avg_height")
).withColumn(
    "timestamp", current_timestamp()
)

df_avg.show()

# Запис фінальної аналітичної таблиці у Gold layer
df_avg.write.mode("overwrite").parquet("gold/avg_stats")

spark.stop()