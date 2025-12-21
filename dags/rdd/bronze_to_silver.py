import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -------------------- logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("bronze_to_silver")
# -------------------------------------------------

def clean_text_columns(df):
    """Чистим все текстовые колонки (trim, lower)"""
    text_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType"]
    for c in text_cols:
        df = df.withColumn(c, F.trim(F.lower(F.col(c))))
    return df

def main(table_name: str):
    logger.info(f"Starting bronze_to_silver job for table: {table_name}")

    spark = SparkSession.builder.appName(f"Bronze to Silver - {table_name}").getOrCreate()
    logger.info("SparkSession created")

    # Путь к bronze
    DAG_DIR = os.path.dirname(os.path.abspath(__file__))
    bronze_path = os.path.join(DAG_DIR, "bronze", table_name)
    if not os.path.exists(bronze_path):
        logger.error(f"Bronze path does not exist: {bronze_path}")
        spark.stop()
        sys.exit(1)

    logger.info(f"Reading bronze table from: {bronze_path}")
    df = spark.read.parquet(bronze_path)

    logger.info(f"Bronze table loaded. Number of rows: {df.count()}")
    df.show(5, truncate=False)  # показываем первые 5 строк

    # Чистим текстовые колонки
    df = clean_text_columns(df)

    # Дедубликация
    df = df.dropDuplicates()
    logger.info(f"After deduplication. Number of rows: {df.count()}")
    df.show(5, truncate=False)

    # Путь к silver
    silver_path = os.path.join(DAG_DIR, "silver", table_name)
    os.makedirs(silver_path, exist_ok=True)

    logger.info(f"Writing DataFrame to silver path: {silver_path}")
    df.write.mode("overwrite").parquet(silver_path)
    logger.info("Write completed successfully")

    spark.stop()
    logger.info("SparkSession stopped")
    logger.info("bronze_to_silver job finished")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: python bronze_to_silver.py <table_name>")
        sys.exit(1)

    main(sys.argv[1])
