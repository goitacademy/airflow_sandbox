# import requests
import os
# import re

from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType

from pyspark.sql.functions import when, col, current_timestamp



# Створюємо сесію Spark
spark = SparkSession.builder.appName("GoldLevel").getOrCreate()


#  gold level

input_folder = 'silver'
output_folder = 'gold'

file_name_list = [
                    'athlete_bio',
                    'athlete_event_results',
                ]
output_file_name = 'avg_stats'

# Створення папки лише якщо вона не існує
os.makedirs(output_folder, exist_ok=True)


# Завантаження Parquet-файлу у DataFrame
bio_df = spark.read.parquet(f'{input_folder}/{file_name_list[0]}')
print(f'Downloaded {input_folder}/{file_name_list[0]}')

# Заміна текстових значень на None перед перетворенням типу даних, фільтрація ненульових значень ваги та зросту
bio_df = bio_df.withColumn(
    "height",
    when(col("height").rlike("^[0-9]+(\\.[0-9]+)?$"), col("height").cast("float")).otherwise(None)
)
bio_df = bio_df.filter(col("height").isNotNull())

bio_df = bio_df.withColumn(
    "weight",
    when(col("weight").rlike("^[0-9]+(\\.[0-9]+)?$"), col("weight").cast("integer")).otherwise(None)
)
bio_df = bio_df.filter(col("weight").isNotNull())
print(f'Cleaned {input_folder}/{file_name_list[0]}')


# Завантаження Parquet-файлу у DataFrame
res_df = spark.read.parquet(f'{input_folder}/{file_name_list[1]}')
print(f'Downloaded {input_folder}/{file_name_list[1]}')

# Виконання left join
df_joined = res_df.join(bio_df.select("athlete_id", "height", "weight", 'sex'), on=['athlete_id'], how='left')#.toPandas()
print(f'Joined {input_folder}/{file_name_list[1]} and {input_folder}/{file_name_list[1]}')

df_grouped = df_joined.groupBy("sport", 'medal', 'sex', 'country_noc').avg("height", 'weight').withColumn('timestamp', current_timestamp()).cache()

df_grouped.show()

#Збереження файлів у папку gold
df_grouped.write.format("parquet").option("compression", "gzip").mode("overwrite").save(f'{output_folder}/{output_file_name}')
print(f'Saved {output_folder}/{output_file_name}.parquet')
df_grouped.show()
