from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, avg, current_timestamp, col, expr, round

spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

athlete_bio = spark.read.parquet("silver/athlete_bio")
athlete_event_results = spark.read.parquet("silver/athlete_event_results")

athlete_bio = athlete_bio \
    .withColumn("height", expr("try_cast(height as double)")) \
    .withColumn("weight", expr("try_cast(weight as double)")) \
    .filter(col("height").isNotNull()) \
    .filter(col("weight").isNotNull())

athlete_bio = athlete_bio.withColumnRenamed("country_noc", "bio_country_noc")

joined = athlete_event_results.join(
    broadcast(athlete_bio),
    on="athlete_id",
    how="inner"
)

gold = joined.groupBy(
    "sport",
    "medal",
    "sex",
    "bio_country_noc"
).agg(
    round(avg("weight"),2).alias("avg_weight"),
    round(avg("height"),2).alias("avg_height")
).withColumn(
    "timestamp",
    current_timestamp()
)

gold.write.mode("overwrite").parquet("gold/avg_stats")

gold.show(10, truncate=False)

print("Silver to Gold Successfully Done")