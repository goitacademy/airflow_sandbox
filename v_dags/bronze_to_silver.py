import requests
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


# Створюємо сесію Spark
spark = SparkSession.builder.appName("silverLevel").getOrCreate()


#  silver level

# Функція для очищення тексту
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

input_folder = 'bronze'
output_folder = 'silver'

file_name_list = [
                    'athlete_bio',
                    'athlete_event_results',
                ]

# Створення папки лише якщо вона не існує
os.makedirs(output_folder, exist_ok=True)

for file in file_name_list:
    
    # Завантаження Parquet-файлу у DataFrame
    temp_df = spark.read.parquet(f'{input_folder}/{file}')
    print(f'Downloaded {input_folder}/{file}')

    # Створіть UDF для очищення тексту
    clean_text_udf = udf(clean_text, StringType())
    
    # Очистіть всі колонки типу string
    for col in temp_df.columns:
        if temp_df.schema[col].dataType == StringType():
            temp_df = temp_df.withColumn(col, clean_text_udf(temp_df[col]))
    print(f'Cleaned {input_folder}/{file}')
    
    # Видалення дублікатів
    temp_df = temp_df.dropDuplicates()
    print(f'Deduplicated {input_folder}/{file}')
    
    #Збереження файлів у папку silver
    temp_df.write.format("parquet").option("compression", "gzip").mode("overwrite").save(f'{output_folder}/{file}')
    print(f'Saved {output_folder}/{file}.parquet')
    temp_df.show()
    