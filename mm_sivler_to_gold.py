from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col, round as spark_round
from pyspark.sql.types import DoubleType
import os

def process_silver_to_gold():
    # Create Spark session
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    # Read silver layer data
    athlete_bio_df = spark.read.parquet("/tmp/silver/athlete_bio")
    athlete_event_results_df = spark.read.parquet("/tmp/silver/athlete_event_results")

    # Rename country_noc column in athlete_bio to avoid conflict
    athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

    # Cast height and weight to DoubleType
    athlete_bio_df = athlete_bio_df.withColumn("height", col("height").cast(DoubleType()))
    athlete_bio_df = athlete_bio_df.withColumn("weight", col("weight").cast(DoubleType()))

    # Join dataframes on athlete_id
    joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

    # Aggregate and round average height and weight to 1 decimal place
    aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            spark_round(avg("height"), 1).alias("avg_height"),
            spark_round(avg("weight"), 1).alias("avg_weight"),
            current_timestamp().alias("timestamp")
        )

    # Write result to gold layer
    output_path = "/tmp/gold/avg_stats"
    os.makedirs(output_path, exist_ok=True)
    aggregated_df.write.mode("overwrite").parquet(output_path)

    print(f"Data saved to {output_path}")

    # Read and display result for logging
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

    spark.stop()

def main():
    process_silver_to_gold()

if __name__ == "__main__":
    main()
