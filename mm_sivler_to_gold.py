from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col, round as spark_round, format_number
from pyspark.sql.types import DoubleType
import os

def process_silver_to_gold():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    athlete_bio_df = spark.read.parquet("/tmp/silver/athlete_bio")
    athlete_event_results_df = spark.read.parquet("/tmp/silver/athlete_event_results")

    athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")
    athlete_bio_df = athlete_bio_df.withColumn("height", col("height").cast(DoubleType()))
    athlete_bio_df = athlete_bio_df.withColumn("weight", col("weight").cast(DoubleType()))

    joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

    # Aggregate as raw double
    aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            avg("height").alias("avg_height_raw"),
            avg("weight").alias("avg_weight_raw"),
            current_timestamp().alias("timestamp")
        )

    # Format to strings with 1 decimal
    result_df = aggregated_df \
        .withColumn("avg_height", format_number(spark_round(col("avg_height_raw"), 1), 1)) \
        .withColumn("avg_weight", format_number(spark_round(col("avg_weight_raw"), 1), 1)) \
        .drop("avg_height_raw", "avg_weight_raw")

    # Save
    output_path = "/tmp/gold/avg_stats"
    os.makedirs(output_path, exist_ok=True)
    result_df.write.mode("overwrite").parquet(output_path)

    print(f"Data saved to {output_path}")

    # Show in logs
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

    spark.stop()

def main():
    process_silver_to_gold()

if __name__ == "__main__":
    main()
