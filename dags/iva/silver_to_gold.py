from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp
from pyspark.sql.types import IntegerType
import os


def run_etl() -> None:
    """
    Silver -> Gold:
    - читаємо silver/athlete_bio та silver/athlete_event_results
    - join по athlete_id
    - групування по (sport, medal, sex, country_noc)
    - середні weight/height
    - колонка processing_timestamp
    - запис у gold/avg_stats
    """
    spark = (
        SparkSession.builder
        .appName("SilverToGold_avg_stats")
        .getOrCreate()
    )

    bio_path = os.path.join("data_lake", "silver", "athlete_bio")
    results_path = os.path.join("data_lake", "silver", "athlete_event_results")

    bio = spark.read.parquet(bio_path)
    results = spark.read.parquet(results_path)

    # Кастинг числових колонок
    bio = bio.withColumn("height", col("height").cast(IntegerType()))
    bio = bio.withColumn("weight", col("weight").cast(IntegerType()))

    # Уникаємо дублювання country_noc: беремо з bio
    bio = bio.withColumnRenamed("country_noc", "country_noc_bio")

    df = results.join(bio, on="athlete_id", how="inner")

    # Видаляємо country_noc з results, залишаємо з bio
    df = df.drop("country_noc")
    df = df.withColumnRenamed("country_noc_bio", "country_noc")

    grouped = (
        df.groupBy("sport", "medal", "sex", "country_noc")
          .agg(
              avg("weight").alias("avg_weight"),
              avg("height").alias("avg_height"),
          )
          .withColumn("processing_timestamp", current_timestamp())
    )

    # df.show() для скріншота
    grouped.show(50, truncate=False)

    output_path = os.path.join("data_lake", "gold", "avg_stats")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    grouped.write.mode("overwrite").parquet(output_path)

    print(f"Gold table saved to {output_path}")
    spark.stop()


if __name__ == "__main__":
    run_etl()


