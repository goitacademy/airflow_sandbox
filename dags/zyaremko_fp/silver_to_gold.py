from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    # Читаємо silver-дані
    bio = spark.read.parquet("silver/athlete_bio")
    results = spark.read.parquet("silver/athlete_event_results")

    # Join по athlete_id
    joined = results.join(bio, on="athlete_id", how="inner")

    # Перетворення типів
    joined = joined.withColumn("height", col("height").cast("double")) \
                   .withColumn("weight", col("weight").cast("double"))

    # Агрегація
    gold = joined.groupBy("sport", "medal", "sex", "country_noc").agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ).withColumn("calculated_at", current_timestamp())

    print("=== Gold aggregated stats ===")
    gold.show(10, truncate=False)

    # Збереження
    output_path = "gold/avg_stats"
    gold.write.mode("overwrite").parquet(output_path)
    print(f"✅ Збережено у {output_path}")
