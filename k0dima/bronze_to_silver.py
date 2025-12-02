# зчитувати таблицю bronze,
# виконувати функцію чистки тексту для всіх текстових колонок,
# робити дедублікацію рядків,
# записувати таблицю в папку.

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import re


# Для очищення текстових колонок варто використати функцію
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))


# Цю python-функцію необхідно загорнути у spark user defined function і використати:
clean_text_udf = udf(clean_text, StringType())

# Створюємо сесію Spark з іменем "BronzeToSilver"
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# тут df - це spark DataFrame
tables = ["athlete_bio", "athlete_event_results"]
for table in tables:
    df = spark.read.parquet(f"/tmp/bronze/{table}")
    for column in df.columns:
        df = df.withColumn(column, clean_text_udf(df[column]))
        
    df = df.dropDuplicates()
    
    output_path = f"/tmp/silver/{table}"
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)

spark.stop()
