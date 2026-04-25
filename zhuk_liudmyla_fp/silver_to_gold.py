"""
Part 2 · silver_to_gold
=======================

1) Зчитує silver/athlete_bio та silver/athlete_event_results.
2) Робить join за athlete_id.
3) Для кожної комбінації (sport, medal, sex, country_noc) рахує avg(weight), avg(height).
4) Додає колонку timestamp (момент виконання програми).
5) Зберігає parquet у gold/avg_stats/.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("SilverToGold")
        .getOrCreate()
    )

    try:
        bio_path = os.path.join("silver", "athlete_bio")
        results_path = os.path.join("silver", "athlete_event_results")

        bio = spark.read.parquet(bio_path)
        results = spark.read.parquet(results_path)

        # Колонки weight/height у CSV — це рядки зі значеннями типу "75.0".
        # Приводимо до double, некоректні значення стають null.
        bio = (
            bio
            .withColumn("weight", col("weight").cast("double"))
            .withColumn("height", col("height").cast("double"))
        )

        # У обох датафреймах є колонка country_noc — треба один варіант.
        # Використовуємо версію з athlete_event_results (факт результатів по країні).
        # Щоб уникнути двозначності, перейменуємо в bio.
        bio = bio.withColumnRenamed("country_noc", "bio_country_noc")

        joined = bio.join(results, on="athlete_id", how="inner")

        agg = (
            joined
            .groupBy("sport", "medal", "sex", "country_noc")
            .agg(
                avg("weight").alias("avg_weight"),
                avg("height").alias("avg_height"),
            )
            .withColumn("timestamp", current_timestamp())
        )

        print("[silver_to_gold] avg_stats:")
        agg.show(truncate=False)

        gold_path = os.path.join("gold", "avg_stats")
        agg.write.mode("overwrite").parquet(gold_path)
        print(f"[silver_to_gold] saved to {gold_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
