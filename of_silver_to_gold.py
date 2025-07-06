from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
from pathlib import Path

spark = SparkSession.builder.appName("SilverToGoldLayer").getOrCreate()

Path("gold").mkdir(parents=True, exist_ok=True)

df_bio = spark.read.parquet("silver/athlete_bio")
df_results = spark.read.parquet("silver/athlete_event_results")

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

df_avg.write.mode("overwrite").parquet("gold/avg_stats")

spark.stop()