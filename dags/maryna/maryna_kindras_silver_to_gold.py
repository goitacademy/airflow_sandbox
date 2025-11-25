from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col, round as spark_round
from pyspark.sql.types import DoubleType
import os

def process():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    bio = spark.read.parquet("/tmp/silver/athlete_bio")
    events = spark.read.parquet("/tmp/silver/athlete_event_results")

    bio = bio.withColumn("height", col("height").cast(DoubleType()))
    bio = bio.withColumn("weight", col("weight").cast(DoubleType()))

    joined = events.join(bio, "athlete_id")

    agg = joined.groupBy("sport", "medal", "sex", "country_noc").agg(
        spark_round(avg("height"), 1).alias("avg_height"),
        spark_round(avg("weight"), 1).alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )

    output_path = "/tmp/gold/avg_stats"
    os.makedirs(output_path, exist_ok=True)

    agg.write.mode("overwrite").parquet(output_path)

    print(f"Saved to {output_path}")
    agg.show(truncate=False)

    spark.stop()


def main():
    process()

if __name__ == "__main__":
    main()
