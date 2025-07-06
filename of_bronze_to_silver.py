import re
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("BronzeToSilverLayer").getOrCreate()

def clean_text(text):
    return re.sub(r"[^a-zA-Z0-9,.\\\"\' ]", '', str(text))

clean_text_udf = udf(clean_text, StringType())

Path("silver").mkdir(parents=True, exist_ok=True)

df_bio = spark.read.parquet("bronze/athlete_bio")
df_results = spark.read.parquet("bronze/athlete_event_results")

df_bio_cleaned = df_bio.withColumn("name", clean_text_udf(df_bio["name"]))
df_results_cleaned = df_results.withColumn("event", clean_text_udf(df_results["event"]))

df_bio_cleaned.write.mode("overwrite").parquet("silver/athlete_bio")
df_results_cleaned.write.mode("overwrite").parquet("silver/athlete_event_results")

df_bio_cleaned.show(3)
df_results_cleaned.show(3)

print(f"Bio rows: {df_bio_cleaned.count()}")
print(f"Results rows: {df_results_cleaned.count()}")

spark.stop()