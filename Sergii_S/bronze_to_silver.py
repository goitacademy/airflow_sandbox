from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
import os

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

def process_table(table):
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    # read the table from the bronze layer
    input_path = f"bronze/{table}"
    df = spark.read.parquet(input_path)

    # clean text and drop duplicates
    clean_text_udf = udf(clean_text, StringType())

    text_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    for col_name in text_columns:
         df = df.withColumn(col_name, clean_text_udf(df[col_name]))
    df_cleaned = df.dropDuplicates()

    # create output directory and save results
    output_path = f"silver/{table}"
    os.makedirs(output_path, exist_ok=True)
    df_cleaned.write.mode("overwrite").parquet(output_path)

    print(f"Successfully processed {table} and saved as {output_path}")

    # show final dataframe for the job
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

    spark.stop()

tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    process_table(table)