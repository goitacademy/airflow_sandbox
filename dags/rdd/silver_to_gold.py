import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp


# -------------------- logging config --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("silver_to_gold")
# --------------------------------------------------------


def main():
    logger.info("Starting silver_to_gold job")

    spark = (
        SparkSession.builder
        .appName("Silver to Gold - avg stats")
        .getOrCreate()
    )

    logger.info("SparkSession created")

    # Paths
    bio_path = "silver/athlete_bio"
    results_path = "silver/athlete_event_results"
    gold_path = "gold/avg_stats"

    logger.info(f"Reading athlete_bio from: {bio_path}")
    bio_df = spark.read.parquet(bio_path)

    logger.info(f"Reading athlete_event_results from: {results_path}")
    results_df = spark.read.parquet(results_path)

    logger.info("Joining tables on athlete_id")

    joined_df = (
        results_df
        .join(bio_df, on="athlete_id", how="inner")
    )

    logger.info("Join completed")

    # Aggregation
    logger.info(
        "Calculating average height and weight "
        "grouped by sport, medal, sex, country_noc"
    )

    agg_df = (
        joined_df
        .groupBy(
            "sport",
            "medal",
            "sex",
            "country_noc"
        )
        .agg(
            avg("weight").alias("avg_weight"),
            avg("height").alias("avg_height")
        )
        .withColumn(
            "timestamp",
            current_timestamp()
        )
    )

    logger.info("Aggregation completed")

    # Write to gold
    logger.info(f"Writing gold table to: {gold_path}")

    (
        agg_df.write
        .mode("overwrite")
        .parquet(gold_path)
    )

    logger.info("Write completed successfully")

    spark.stop()
    logger.info("SparkSession stopped")
    logger.info("silver_to_gold job finished")


if __name__ == "__main__":
    main()
