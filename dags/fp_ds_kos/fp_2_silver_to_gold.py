import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col, expr, round, lower, trim, when

spark = SparkSession.builder.appName("SilverToGold").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SILVER_DIR = os.path.join(BASE_DIR, "silver")
GOLD_DIR = os.path.join(BASE_DIR, "gold")

athlete_bio_path = os.path.join(SILVER_DIR, "athlete_bio")
athlete_event_results_path = os.path.join(SILVER_DIR, "athlete_event_results")
gold_output_path = os.path.join(GOLD_DIR, "avg_stats")

print(f"Reading athlete_bio from: {athlete_bio_path}")
athlete_bio = spark.read.parquet(athlete_bio_path)

print(f"Reading athlete_event_results from: {athlete_event_results_path}")
athlete_event_results = spark.read.parquet(athlete_event_results_path)

athlete_bio = athlete_bio \
    .withColumn("height", expr("try_cast(height as double)")) \
    .withColumn("weight", expr("try_cast(weight as double)")) \
    .filter(col("height").isNotNull()) \
    .filter(col("weight").isNotNull())

athlete_bio = athlete_bio.withColumnRenamed("country_noc", "bio_country_noc")

joined = athlete_event_results.join(
    athlete_bio,
    on="athlete_id",
    how="inner"
)

joined = joined.withColumn(
    "medal",
    when(
        col("medal").isNull() |
        lower(trim(col("medal").cast("string"))).isin("nan", "none", "null", ""),
        "No medal"
    ).otherwise(col("medal"))
)

gold = joined.groupBy(
    "sport",
    "medal",
    "sex",
    "bio_country_noc"
).agg(
    round(avg("weight"), 2).alias("avg_weight"),
    round(avg("height"), 2).alias("avg_height")
).withColumn(
    "timestamp",
    current_timestamp()
)

gold = gold.withColumnRenamed("bio_country_noc", "country_noc")

print(f"Writing gold to: {gold_output_path}")
gold.write.mode("overwrite").parquet(gold_output_path)

gold.show(10, truncate=False)

spark.stop()

print("Silver to Gold Successfully Done")