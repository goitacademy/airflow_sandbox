
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, udf
from pyspark.sql.types import StringType
import os
import re

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())

def clean_text_columns(df):
    for column in df.columns:
        if df.schema[column].dataType == StringType():
            df = df.withColumn(column, clean_text_udf(trim(lower(col(column)))))
    return df

def process_table(table):
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    input_path = f"/tmp/bronze/{table}"
    df = spark.read.parquet(input_path)

    df = clean_text_columns(df)
    df = df.dropDuplicates()

    output_path = f"/tmp/silver/{table}"
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)

    print(f"✅ Дані збережено у {output_path}")

    df = spark.read.parquet(output_path)
    df.show(truncate=False)

    spark.stop()

def main():
    tables = ["athlete_bio", "athlete_event_results"]
    for table in tables:
        process_table(table)

if __name__ == "__main__":
    main()
