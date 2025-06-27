
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col
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

    aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight"),
            current_timestamp().alias("timestamp")
        )

    output_path = "/tmp/gold/avg_stats"
    os.makedirs(output_path, exist_ok=True)
    aggregated_df.write.mode("overwrite").parquet(output_path)

    print(f"Data saved to {output_path}")

    df = spark.read.parquet(output_path)
    df.show(truncate=False)

    spark.stop()

def main():
    process_silver_to_gold()

if __name__ == "__main__":
    main()
