import re
import os
import sys
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, lower, regexp_replace
from pyspark.sql.types import StringType

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))


def apply_clean_text_to_all_string_columns(df):
    # Get all columns that have type StringType
    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]
    print(f"Text columns for cleaning: {string_columns}")
    clean_text_udf = udf(clean_text, StringType())

    # Apply UDF for each column
    for col_name in string_columns:
        df = df.withColumn(col_name, clean_text_udf(df[col_name]))

    return df

if __name__ == "__main__":
    # Init Spark Session
    spark = SparkSession.builder \
            .appName(f"BronzeToSilverPipeline") \
            .getOrCreate()

    # Add paths to files
    input_path_bio = os.path.join("bronze", "athlete_bio.parquet")
    input_path_res = os.path.join("bronze", "athlete_event_results.parquet")
    output_path_bio = os.path.join("silver", "athlete_bio.parquet")
    output_path_res = os.path.join("silver", "athlete_event_results.parquet")

    try:
        # Read the data from files
        df_bio_bronze = spark.read.parquet(input_path_bio)
        df_res_bronze = spark.read.parquet(input_path_res)

        # Clean text in the data
        print("Cleaning text in the data")
        df_bio_cleaned = apply_clean_text_to_all_string_columns(df_bio_bronze)
        df_res_cleaned = apply_clean_text_to_all_string_columns(df_res_bronze)

        # Reduplication of rows
        print("Reduplication of rows")
        df_bio_silver = df_bio_cleaned.dropDuplicates()
        df_res_silver = df_res_cleaned.dropDuplicates()
        df_bio_silver.show(20)
        df_res_silver.show(20)

        # Write table in folder silver/{table} in format Parquet
        print("Saving silver data")
        df_bio_silver.write.mode("overwrite").parquet(output_path_bio)
        df_res_silver.write.mode("overwrite").parquet(output_path_res)

        print(f"Process Bronze -> Silver finished successfully.")

    except Exception as e:
        print(f"{e}")
        sys.exit(1)

    finally:
        # Stop Spark Session
        spark.stop()
        print("Spark Session stopped.")