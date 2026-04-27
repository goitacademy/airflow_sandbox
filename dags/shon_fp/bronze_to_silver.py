import sys
import re
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))


clean_text_udf = udf(clean_text, StringType())


def main(table_name: str):
    print(f"Starting bronze_to_silver for table: {table_name}")
    print("Current working directory:", os.getcwd())
    spark = (
        SparkSession.builder
        .appName(f"BronzeToSilver_{table_name}")
        .getOrCreate()
    )

    input_path = f"bronze/{table_name}"
    output_path = f"silver/{table_name}"

    df = spark.read.parquet(input_path)

    print("Bronze schema:")
    df.printSchema()

    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    print(f"String columns to clean: {string_columns}")

    for column_name in string_columns:
        df = df.withColumn(column_name, clean_text_udf(col(column_name)))

    df_cleaned = df.dropDuplicates()

    print("Silver preview:")
    df_cleaned.show(10, truncate=False)

    (
        df_cleaned.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Saved to: {output_path}")
    print("Checking if output exists:", os.path.exists(output_path))
    print("Absolute output path:", os.path.abspath(output_path))
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("Table name is required. Example: python bronze_to_silver.py athlete_bio")

    main(sys.argv[1])