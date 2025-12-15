from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SILVER_DIR = BASE_DIR / "silver"
GOLD_DIR = BASE_DIR / "gold"

spark = SparkSession.builder.appName("SilverToGoldLayer").getOrCreate()

GOLD_DIR.mkdir(parents=True, exist_ok=True)

df_bio = spark.read.parquet(str(SILVER_DIR / "athlete_bio"))
df_results = spark.read.parquet(str(SILVER_DIR / "athlete_event_results"))

df_joined = df_results.join(df_bio, on="athlete_id", how="inner")

df_avg = df_joined.groupBy(
    "sport",
    "medal",
    "sex",
    df_bio["country_noc"]
).agg(
    avg("weight").alias("avg_weight"),
    avg("height").alias("avg_height")
).withColumn(
    "timestamp", current_timestamp()
)

df_avg.show()

df_avg.write.mode("overwrite").parquet(str(GOLD_DIR / "avg_stats"))

spark.stop()
