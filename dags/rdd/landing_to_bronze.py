import sys
import os
import logging
import requests
from pyspark.sql import SparkSession

# -------------------- logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("landing_to_bronze")
# -------------------------------------------------

def download_csv(url: str, local_path: str):
    """Скачивает CSV с URL в локальный файл"""
    logger.info(f"Downloading CSV from {url} to {local_path}")
    r = requests.get(url)
    r.raise_for_status()  # если ошибка HTTP, падение
    with open(local_path, "wb") as f:
        f.write(r.content)
    logger.info("Download completed")
    return local_path


def main(table_name: str):
    logger.info(f"Starting landing_to_bronze job for table: {table_name}")

    spark = SparkSession.builder.appName(f"Landing to Bronze - {table_name}").getOrCreate()
    logger.info("SparkSession created")

    # Определяем URL для скачивания
    urls = {
        "athlete_bio": "https://ftp.goit.study/neoversity/athlete_bio.csv",
        "athlete_event_results": "https://ftp.goit.study/neoversity/athlete_event_results.csv"
    }

    if table_name not in urls:
        logger.error(f"Unknown table_name: {table_name}")
        spark.stop()
        sys.exit(1)

    url = urls[table_name]

    # Локальный файл внутри DAG
    DAG_DIR = os.path.dirname(os.path.abspath(__file__))
    local_csv_path = os.path.join(DAG_DIR, f"{table_name}.csv")

    # Скачиваем CSV
    download_csv(url, local_csv_path)

    # Читаем CSV через Spark
    logger.info("Reading CSV into Spark DataFrame")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(local_csv_path)
    
    logger.info(f"CSV loaded. Number of rows: {df.count()}")
    df.show(5, truncate=False)  # показываем первые 5 строк

    # Путь для сохранения bronze
    bronze_path = os.path.join(DAG_DIR, "bronze", table_name)
    os.makedirs(bronze_path, exist_ok=True)

    logger.info(f"Writing DataFrame to bronze path: {bronze_path}")
    df.write.mode("overwrite").parquet(bronze_path)
    logger.info("Write completed successfully")

    spark.stop()
    logger.info("SparkSession stopped")
    logger.info("landing_to_bronze job finished")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: python landing_to_bronze.py <table_name>")
        sys.exit(1)

    main(sys.argv[1])
