from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import re
import sys
import os


def clean_text(text: str) -> str:
    """
    Очищення тексту від символів поза a-zA-Z0-9,.,'," (як у вимозі)
    """
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))


clean_text_udf = udf(clean_text, StringType())


def run_etl(table_name: str) -> None:
    """
    Bronze -> Silver:
    - читаємо parquet із bronze
    - чистимо всі текстові колонки
    - дедублікація
    - пишемо у silver/{table}
    """
    spark = (
        SparkSession.builder
        .appName(f"BronzeToSilver_{table_name}")
        .getOrCreate()
    )

    input_path = os.path.join("data_lake", "bronze", table_name)
    df = spark.read.parquet(input_path)

    # Чистимо всі StringType-колонки
    for c in df.columns:
        if isinstance(df.schema[c].dataType, StringType):
            df = df.withColumn(c, clean_text_udf(col(c)))

    # Дедублікація
    df = df.dropDuplicates()

    # df.show() для скріншота
    df.show(20, truncate=False)

    output_path = os.path.join("data_lake", "silver", table_name)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)

    print(f"Silver table saved to {output_path}")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: bronze_to_silver.py <table_name>")
    table = sys.argv[1]
    run_etl(table)

