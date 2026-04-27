from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, when


def main():
    print("Starting silver_to_gold")

    spark = (
        SparkSession.builder
        .appName("SilverToGold_AvgStats")
        .getOrCreate()
    )

    # Етап 1: зчитування двох таблиць із silver layer
    athlete_bio = spark.read.parquet("silver/athlete_bio")
    athlete_event_results = spark.read.parquet("silver/athlete_event_results")

    print("athlete_bio schema:")
    athlete_bio.printSchema()

    print("athlete_event_results schema:")
    athlete_event_results.printSchema()

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

    print("Gold avg_stats preview:")
    avg_stats.show(50, truncate=False)

    # Етап 6: запис результату у gold layer
    (
        avg_stats.write
        .mode("overwrite")
        .parquet("gold/avg_stats")
    )

    print("Saved to: gold/avg_stats")

    spark.stop()


if __name__ == "__main__":
    main()