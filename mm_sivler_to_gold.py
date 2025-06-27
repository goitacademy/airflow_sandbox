from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col, round as spark_round, format_number
from pyspark.sql.types import DoubleType
import os

def process_silver_to_gold():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    # Read silver layer data
    athlete_bio_df = spark.read.parquet("/tmp/silver/athlete_bio")
    athlete_event_results_df = spark.read.parquet("/tmp/silver/athlete_event_results")

    # Rename to avoid conflict
    athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

    # Cast to double
    athlete_bio_df = athlete_bio_df.withColumn("height", col("height").cast(DoubleType()))
    athlete_bio_df = athlete_bio_df.withColumn("weight", col("weight").cast(DoubleType()))

    # Join
    joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

    # Aggregate
    aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            avg("height").alias("avg_height_raw"),
            avg("weight").alias("avg_weight_raw"),
            current_timestamp().alias("timestamp")
        )

    # Round and format for display
    aggregated_df = aggregated_df \
        .withColumn("avg_height", format_number(spark_round(col("avg_height_raw"), 1), 1)) \
        .withColumn("avg_weight", format_number(spark_round(col("avg_weight_raw"), 1), 1)) \
        .drop("avg_height_raw", "avg_weight_raw")

    # Write to gold
    output_path = "/tmp/gold/avg_stats"
    os.makedirs(output_path, exist_ok=True)
    aggregated_df.write.mode("overwrite").parquet(output_path)

    print(f"Data saved to {output_path}")

    # Read back and show
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

    spark.stop()

def main():
    process_silver_to_gold()

if __name__ == "__main__":
    main()
