import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, when


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def main():
    spark = (
        SparkSession.builder
        .appName("SilverToGold_AvgStats")
        .getOrCreate()
    )

    athlete_bio_path = os.path.join(BASE_DIR, "silver", "athlete_bio")
    athlete_event_results_path = os.path.join(BASE_DIR, "silver", "athlete_event_results")
    gold_path = os.path.join(BASE_DIR, "gold", "avg_stats")

    # Етап 1: зчитування двох таблиць із silver layer
    athlete_bio = spark.read.parquet(athlete_bio_path)
    athlete_event_results = spark.read.parquet(athlete_event_results_path)

    # Етап 2: фільтрація height/weight — залишаємо тільки числові значення
    athlete_bio_cleaned = (
        athlete_bio
        .withColumn("height_num", col("height").cast("double"))
        .withColumn("weight_num", col("weight").cast("double"))
        .filter(col("height_num").isNotNull())
        .filter(col("weight_num").isNotNull())
    )

    # Етап 3: join athlete_event_results + athlete_bio по athlete_id
    joined_df = (
        athlete_event_results.alias("r")
        .join(
            athlete_bio_cleaned.alias("b"),
            col("r.athlete_id") == col("b.athlete_id"),
            "inner"
        )
    )

    # Етап 4: обробка пустих medal
    joined_df = joined_df.withColumn(
        "medal_clean",
        when(
            col("r.medal").isNull() | (col("r.medal") == ""),
            "No medal"
        ).otherwise(col("r.medal"))
    )

    # Етап 5: розрахунок середнього height/weight
    avg_stats = (
        joined_df
        .groupBy(
            col("r.sport").alias("sport"),
            col("medal_clean").alias("medal"),
            col("b.sex").alias("sex"),
            col("b.country_noc").alias("country_noc")
        )
        .agg(
            avg(col("height_num")).alias("avg_height"),
            avg(col("weight_num")).alias("avg_weight")
        )
        .withColumn("timestamp", current_timestamp())
    )

    # Мінімальний вивід для скриншота результату
    avg_stats.show(50, truncate=False)

    # Етап 6: запис результату у gold layer
    (
        avg_stats.write
        .mode("overwrite")
        .parquet(gold_path)
    )

    print("Gold avg_stats saved successfully")

    spark.stop()


if __name__ == "__main__":
    main()