import sys
import logging
from pyspark.sql import SparkSession


# -------------------- logging config --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("landing_to_bronze")
# --------------------------------------------------------


def main(table_name: str):
    logger.info("Starting landing_to_bronze job")
    logger.info(f"Table name: {table_name}")

    spark = (
        SparkSession.builder
        .appName(f"Landing to Bronze - {table_name}")
        .getOrCreate()
    )

    logger.info("SparkSession created")

    # URL to source CSV
    url = f"https://ftp.goit.study/neoversity/{table_name}.csv"
    logger.info(f"Source URL: {url}")

    # Read CSV
    logger.info("Reading CSV from source")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(url)
    )

    logger.info(f"CSV loaded successfully. Rows count: {df.count()}")

    # Write to bronze (relative path)
    output_path = f"bronze/{table_name}"
    logger.info(f"Writing data to parquet. Output path: {output_path}")

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    logger.info("Write completed successfully")

    spark.stop()
    logger.info("SparkSession stopped")
    logger.info("landing_to_bronze job finished")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error(
            "Invalid arguments. Usage: python landing_to_bronze.py <table_name>"
        )
        sys.exit(1)

    table = sys.argv[1]
    main(table)
