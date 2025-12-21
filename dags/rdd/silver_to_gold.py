import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -------------------- logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("silver_to_gold")
# -------------------------------------------------

def main():
    logger.info("Starting silver_to_gold job")

    spark = SparkSession.builder.appName("Silver to Gold - avg_stats").getOrCreate()
    logger.info("SparkSession created")

    # -------------------- paths --------------------
    DAG_DIR = os.path.dirname(os.path.abspath(__file__))
    bio_path = os.path.join(DAG_DIR, "silver", "athlete_bio")
    results_path = os.path.join(DAG_DIR, "silver", "athlete_event_results")
    gold_path = os.path.join(DAG_DIR, "gold", "avg_stats")
    os.makedirs(gold_path, exist_ok=True)
    # ----------------------------------------------

    # Чтение silver таблиц
    logger.info(f"Reading athlete_bio from: {bio_path}")
    bio_df = spark.read.parquet(bio_path)
    logger.info(f"athlete_bio rows: {bio_df.count()}")
    bio_df.show(5, truncate=False)

    logger.info(f"Reading athlete_event_results from: {results_path}")
    results_df = spark.read.parquet(results_path)
    logger.info(f"athlete_event_results rows: {results_df.count()}")
    results_df.show(5, truncate=False)

    # Join по athlete_id
    logger.info("Joining tables on athlete_id")
    joined_df = results_df.join(bio_df, on="athlete_id", how="inner")
    logger.info(f"Joined rows: {joined_df.count()}")
    joined_df.show(5, truncate=False)

    # Средние значения по комбинации sport, medal, sex, country_noc
    logger.info("Calculating average weight and height")
    avg_df = (
        joined_df.groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            F.avg("weight").alias("avg_weight"),
            F.avg("height").alias("avg_height")
        )
        .withColumn("timestamp", F.current_timestamp())
    )

    logger.info("Resulting avg_stats DataFrame:")
    avg_df.show(5, truncate=False)

    # Сохраняем в gold
    logger.info(f"Writing avg_stats to: {gold_path}")
    avg_df.write.mode("overwrite").parquet(gold_path)
    logger.info("Write completed successfully")

    spark.stop()
    logger.info("SparkSession stopped")
    logger.info("silver_to_gold job finished")

if __name__ == "__main__":
    main()
