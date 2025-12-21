import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, regexp_replace
from pyspark.sql.types import StringType, NumericType


# -------------------- logging config --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("bronze_to_silver")
# --------------------------------------------------------


def clean_columns(df):
    """
    Cleaning rules:
    - string columns: trim, lower, remove extra spaces
    - numeric columns: cast to double, filter not null
    """
    numeric_columns = []

    for field in df.schema.fields:
        col_name = field.name

        # ---- string columns ----
        if isinstance(field.dataType, StringType):
            logger.info(f"Cleaning text column: {col_name}")
            df = df.withColumn(
                col_name,
                regexp_replace(
                    lower(trim(col(col_name))),
                    r"\s+",
                    " "
                )
            )

        # ---- numeric columns ----
        elif isinstance(field.dataType, NumericType):
            logger.info(f"Casting numeric column to double: {col_name}")
            df = df.withColumn(
                col_name,
                col(col_name).cast("double")
            )
            numeric_columns.append(col_name)

    # ---- filter numeric not null (like your example) ----
    if numeric_columns:
        logger.info(
            f"Filtering rows with NULLs in numeric columns: {numeric_columns}"
        )
        condition = None
        for c in numeric_columns:
            expr = col(c).isNotNull()
            condition = expr if condition is None else condition & expr

        df = df.filter(condition)

    return df


def main(table_name: str):
    logger.info("Starting bronze_to_silver job")
    logger.info(f"Table name: {table_name}")

    spark = (
        SparkSession.builder
        .appName(f"Bronze to Silver - {table_name}")
        .getOrCreate()
    )

    bronze_path = f"bronze/{table_name}"
    silver_path = f"silver/{table_name}"

    logger.info(f"Reading bronze table from: {bronze_path}")
    df = spark.read.parquet(bronze_path)

    logger.info(f"Bronze rows count: {df.count()}")

    # Cleaning
    logger.info("Starting columns cleaning")
    df_cleaned = clean_columns(df)

    # Deduplication
    logger.info("Starting deduplication")
    before = df_cleaned.count()
    df_dedup = df_cleaned.dropDuplicates()
    after = df_dedup.count()

    logger.info(f"Rows before dedup: {before}, after dedup: {after}")

    # Write silver
    logger.info(f"Writing silver table to: {silver_path}")
    (
        df_dedup.write
        .mode("overwrite")
        .parquet(silver_path)
    )

    spark.stop()
    logger.info("bronze_to_silver job finished")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error(
            "Usage: python bronze_to_silver.py <table_name>"
        )
        sys.exit(1)

    main(sys.argv[1])
