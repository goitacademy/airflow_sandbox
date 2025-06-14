import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp

spark = SparkSession.builder.appName("Silver_to_Gold").getOrCreate()

df_athlets_bio = spark.read.parquet("/tmp/silver/athlete_bio")
df_athlets_bio = df_athlets_bio.withColumnRenamed("country_noc", "bio_country_noc")
df_athlets_res = spark.read.parquet("/tmp/silver/athlete_event_results")
df_athlets_res = df_athlets_res.withColumnRenamed("country_noc", "event_country_noc")

df = df_athlets_res.join(df_athlets_bio, "athlete_id", "inner")
df = df.withColumn("country_noc", col("bio_country_noc")).drop("bio_country_noc", "event_country_noc")

avg_df = df.groupBy(
    "sport", "medal", "sex", "country_noc"
).agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp"),
)

output_path = f"/tmp/golg/avg_stats"
os.makedirs(output_path, exist_ok=True)
avg_df.write.mode("overwrite").parquet(output_path)
avg_df = spark.read.parquet(output_path)
avg_df.show(truncate=False)

spark.stop()