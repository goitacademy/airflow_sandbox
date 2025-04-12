from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, avg, col, round
from pyspark.sql.types import FloatType
import os

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Silver to Gold") \
        .getOrCreate()

    # Read silver tables
    athlete_bio_df = spark.read.parquet("silver/athlete_bio")
    athlete_results_df = spark.read.parquet("silver/athlete_event_results")

    # Ensure weight and height are numeric
    athlete_bio_df = athlete_bio_df.withColumn("weight", col("weight").cast(FloatType())) \
                                   .withColumn("height", col("height").cast(FloatType()))

    # To avoid ambiguity with the country_noc column during the join,
    # we can create aliases for the DataFrames
    bio_df = athlete_bio_df.alias("bio")
    results_df = athlete_results_df.alias("results")

    # Join tables on athlete_id with specified column references
    joined_df = bio_df.join(
        results_df,
        bio_df.athlete_id == results_df.athlete_id,
        "inner"
    ).select(
        results_df.sport,
        results_df.medal,
        bio_df.sex,
        bio_df.country_noc,
        bio_df.weight,
        bio_df.height
    )

    print("Joined DF count:", joined_df.count())
    joined_df.show()


    # Group by required columns and calculate averages
    gold_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        round(avg("weight"), 2).alias("avg_weight"),
        round(avg("height"), 2).alias("avg_height")
    )

    # Add timestamp column
    gold_df = gold_df.withColumn("timestamp", current_timestamp())

    # Create gold directory if it doesn't exist
    gold_path = "gold/avg_stats"
    os.makedirs(gold_path, exist_ok=True)

    # Save to gold
    gold_df.write.mode("overwrite").parquet(gold_path)

    # Show DataFrame
    print("Saved to gold layer:")
    gold_df.show()

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()