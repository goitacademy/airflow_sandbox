import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

def bronze_to_silver():
    spark = SparkSession.builder \
        .appName("Bronze to Silver") \
        .getOrCreate()

    # Read from Bronze
    athlete_bio_df = spark.read.parquet("bronze/athlete_bio")
    athlete_event_results_df = spark.read.parquet("bronze/athlete_event_results")

    # Define UDF for cleaning text
    clean_text_udf = udf(clean_text, StringType())

    # Clean text columns
    for column in athlete_bio_df.columns:
        athlete_bio_df = athlete_bio_df.withColumn(column, clean_text_udf(col(column)))

    for column in athlete_event_results_df.columns:
        athlete_event_results_df = athlete_event_results_df.withColumn(column, clean_text_udf(col(column)))

    # Drop duplicates
    athlete_bio_cleaned = athlete_bio_df.dropDuplicates()
    athlete_event_results_cleaned = athlete_event_results_df.dropDuplicates()

    # Write to Silver
    athlete_bio_cleaned.write.mode("overwrite").parquet("silver/athlete_bio")
    athlete_event_results_cleaned.write.mode("overwrite").parquet("silver/athlete_event_results")

    spark.stop()

if __name__ == "__main__":
    bronze_to_silver()
