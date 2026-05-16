import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Динамічні шляхи
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "silver")

spark = SparkSession.builder.appName("BronzeToSilverLayer").getOrCreate()

def clean_text(text):
    if text is None:
        return None
    return re.sub(r"[^a-zA-Z0-9,.\\\"\' ]", '', str(text))

clean_text_udf = udf(clean_text, StringType())

os.makedirs(SILVER_DIR, exist_ok=True)

# Читаємо з bronze з урахуванням BASE_DIR
df_bio = spark.read.parquet(os.path.join(BRONZE_DIR, "athlete_bio"))
df_results = spark.read.parquet(os.path.join(BRONZE_DIR, "athlete_event_results"))

# Очищення тексту та обов'язкова дедублікація (dropDuplicates)
for col_name, col_type in df_bio.dtypes:
    if col_type == "string":
        df_bio = df_bio.withColumn(col_name, clean_text_udf(df_bio[col_name]))
df_bio_cleaned = df_bio.dropDuplicates()

for col_name, col_type in df_results.dtypes:
    if col_type == "string":
        df_results = df_results.withColumn(col_name, clean_text_udf(df_results[col_name]))
df_results_cleaned = df_results.dropDuplicates()

# Запис у Silver layer
bio_silver_path = os.path.join(SILVER_DIR, "athlete_bio")
results_silver_path = os.path.join(SILVER_DIR, "athlete_event_results")

df_bio_cleaned.write.mode("overwrite").parquet(bio_silver_path)
df_results_cleaned.write.mode("overwrite").parquet(results_silver_path)

df_bio_cleaned.show(3)
df_results_cleaned.show(3)

spark.stop()