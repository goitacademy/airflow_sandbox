import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col
from pyspark.sql.types import DoubleType

# Динамічні шляхи
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SILVER_DIR = os.path.join(BASE_DIR, "silver")
GOLD_DIR = os.path.join(BASE_DIR, "gold")

spark = SparkSession.builder.appName("SilverToGoldLayer").getOrCreate()

os.makedirs(GOLD_DIR, exist_ok=True)

df_bio = spark.read.parquet(os.path.join(SILVER_DIR, "athlete_bio"))
df_results = spark.read.parquet(os.path.join(SILVER_DIR, "athlete_event_results"))

# Приведення типів до чисельних (вимога ТЗ)
df_bio = df_bio.withColumn("weight", col("weight").cast(DoubleType())) \
               .withColumn("height", col("height").cast(DoubleType()))

# Видаляємо дублікат колонки country_noc з df_bio, щоб уникнути конфліктів при джойні
df_bio_for_join = df_bio.drop("country_noc")

# Об'єднання таблиць (Inner Join)
df_joined = df_results.join(df_bio_for_join, on="athlete_id", how="inner")

# Розрахунок середніх показників
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
gold_path = os.path.join(GOLD_DIR, "avg_stats")
df_avg.write.mode("overwrite").parquet(gold_path)

print(f"Saved to {gold_path}")

spark.stop()