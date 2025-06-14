from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp

spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bio_df = spark.read.parquet("silver/athlete_bio")
event_df = spark.read.parquet("silver/athlete_event_results")

joined_df = (
    event_df.alias("e")
    .join(bio_df.alias("b"), col("e.athlete_id") == col("b.athlete_id"), "inner")
)

agg_df = (
    joined_df
    .groupBy("e.sport", "e.medal", "b.sex", "b.country_noc")
    .agg(
        avg(col("b.height").cast("float")).alias("avg_height"),
        avg(col("b.weight").cast("float")).alias("avg_weight")
    )
    .withColumn("timestamp", current_timestamp())
)

agg_df.write.mode("overwrite").parquet("gold/avg_stats")

agg_df.show()
