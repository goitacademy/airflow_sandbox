from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col

# Stage 3. Silver → Gold
# Join bio + results, aggregate avg height/weight

spark = SparkSession.builder \
    .appName("Silver to Gold") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read silver tables
athlete_bio_df    = spark.read.parquet("silver/athlete_bio")
event_results_df  = spark.read.parquet("silver/athlete_event_results")

# Filter out non-numeric height/weight
athlete_bio_df = athlete_bio_df \
    .filter(col("height").isNotNull() & col("weight").isNotNull()) \
    .filter(col("height").cast("double").isNotNull()) \
    .filter(col("weight").cast("double").isNotNull()) \
    .withColumn("height", col("height").cast("double")) \
    .withColumn("weight", col("weight").cast("double"))

# Join on athlete_id
joined_df = event_results_df.join(
    athlete_bio_df,
    on="athlete_id",
    how="inner"
)

# Avg height & weight per sport / medal / sex / country_noc + timestamp
gold_df = joined_df \
    .groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
    ) \
    .withColumn("timestamp", current_timestamp())

print("=== avg_stats (gold) ===")
gold_df.show(20, truncate=False)
print(f"Row count: {gold_df.count()}")

# Write to gold
gold_df.write.mode("overwrite").parquet("gold/avg_stats")
print("Written to gold/avg_stats")

spark.stop()
