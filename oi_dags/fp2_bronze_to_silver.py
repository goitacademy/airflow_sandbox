# файл bronze_to_silver.py. Він має:
# зчитувати таблицю bronze,
# виконувати функцію чистки тексту для всіх текстових колонок,
# робити дедублікацію рядків,
# записувати таблицю в папку silver/{table}, де {table} — ім’я таблиці..

import re
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


#Для очищення текстових колонок варто використати функцію
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

# Цю python-функцію необхідно загорнути у spark user defined function і використати:
clean_text_udf = udf(clean_text, StringType())

# тут df - це spark DataFrame
# df = df.withColumn(col_name, clean_text_udf(df[col_name]))


def clean_string_columns(df):
    """
    Проходить по всіх колонках DataFrame і
    застосовує clean_text_udf до колонок типу string.
    """
    for name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(name, clean_text_udf(col(name)))
    return df


def process_table(spark, table_name: str):
    # 1. Читання з bronze/{table}
    bronze_path = f"bronze/{table_name}"
    print(f"Читання таблиці з {bronze_path}")
    df = spark.read.parquet(bronze_path)

    # 2. Очищення всіх текстових колонок
    df_clean = clean_string_columns(df)
    df.printSchema()
    df.show(5)

    # 3. Дедублікація рядків
    df_dedup = df_clean.dropDuplicates()

    # 4. Запис у silver/{table}
    silver_path = f"silver/{table_name}"
    print(f"Запис таблиці у {silver_path}")
    (
        df_dedup.write
        .mode("overwrite")
        .parquet(silver_path)
    )



spark = (
    SparkSession.builder
    .appName("BronzeToSilver") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Якщо таблички фіксовані:
tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    process_table(spark, table)

spark.stop()
print("Готово: таблиці записано в silver/*")
