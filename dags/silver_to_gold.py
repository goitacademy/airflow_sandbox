from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp

def silver_to_gold():
    spark = SparkSession.builder \
        .appName("Silver to Gold") \
        .getOrCreate()

    # Read from Silver
    athlete_bio_df = spark.read.parquet("silver/athlete_bio")
    athlete_event_results_df = spark.read.parquet("silver/athlete_event_results")

    # Join tables on athlete_id
    joined_df = athlete_bio_df.join(athlete_event_results_df, "athlete_id")

    # Ensure required columns are converted to numeric type
    joined_df = joined_df.withColumn("weight", joined_df["weight"].cast("float")) \
                         .withColumn("height", joined_df["height"].cast("float"))

    # Group by sport, medal, sex, country_noc and calculate averages
    result_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
        avg("weight").alias("avg_weight"),
        avg("height").alias("avg_height")
    ).withColumn("timestamp", current_timestamp())

    # Write to Gold
    result_df.write.mode("overwrite").parquet("gold/avg_stats")

    spark.stop()

if __name__ == "__main__":
    silver_to_gold()
