import re
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("BronzeToSilverLayer").getOrCreate()

def clean_text(text):
    if text is None:
        return None
    return re.sub(r"[^a-zA-Z0-9,.\\\"\' ]", '', str(text))

clean_text_udf = udf(clean_text, StringType())

Path("silver").mkdir(parents=True, exist_ok=True)

df_bio = spark.read.parquet("bronze/athlete_bio")
df_results = spark.read.parquet("bronze/athlete_event_results")

# Очищення всіх текстових колонок для athlete_bio
for col_name, col_type in df_bio.dtypes:
    if col_type == "string":
        df_bio = df_bio.withColumn(col_name, clean_text_udf(df_bio[col_name]))
df_bio_cleaned = df_bio.dropDuplicates()

# Очищення всіх текстових колонок для athlete_event_results
for col_name, col_type in df_results.dtypes:
    if col_type == "string":
        df_results = df_results.withColumn(col_name, clean_text_udf(df_results[col_name]))
df_results_cleaned = df_results.dropDuplicates()

# Запис у Silver layer
df_bio_cleaned.write.mode("overwrite").parquet("silver/athlete_bio")
df_results_cleaned.write.mode("overwrite").parquet("silver/athlete_event_results")

df_bio_cleaned.show(3)
df_results_cleaned.show(3)

print(f"Bio rows after deduplication: {df_bio_cleaned.count()}")
print(f"Results rows after deduplication: {df_results_cleaned.count()}")

spark.stop()