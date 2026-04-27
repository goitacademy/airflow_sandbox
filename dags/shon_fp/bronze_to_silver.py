import os
import sys
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', "", str(text))


clean_text_udf = udf(clean_text, StringType())


def main(table_name: str):
    spark = (
        SparkSession.builder
        .appName(f"BronzeToSilver_{table_name}")
        .getOrCreate()
    )

    input_path = os.path.join(BASE_DIR, "bronze", table_name)
    output_path = os.path.join(BASE_DIR, "silver", table_name)

    # Етап 1: читання таблиці з bronze layer
    df = spark.read.parquet(input_path)

    # Етап 2: очищення всіх текстових колонок
    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    for column_name in string_columns:
        df = df.withColumn(column_name, clean_text_udf(col(column_name)))

    # Етап 3: дедублікація рядків
    df_cleaned = df.dropDuplicates()

    # Етап 4: запис у silver layer
    (
        df_cleaned.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Table {table_name} saved to silver layer")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception(
            "Table name is required. Example: python bronze_to_silver.py athlete_bio"
        )

    main(sys.argv[1])