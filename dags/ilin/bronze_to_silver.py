from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import re


def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\' ]', '', str(text))

clean_text_udf = udf(clean_text, StringType())

spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    df = spark.read.parquet(f"bronze/{table}")


    for column in df.columns:
        if dict(df.dtypes)[column] == "string":
            df = df.withColumn(column, clean_text_udf(col(column)))

    df_cleaned = df.dropDuplicates()

    df_cleaned.write.mode("overwrite").parquet(f"silver/{table}")
