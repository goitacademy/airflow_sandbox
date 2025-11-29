from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import os
import re


# Функція очищення тексту
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))


clean_text_udf = udf(clean_text, StringType())


# Створення SparkSession
spark = (
    SparkSession.builder
    .appName("BronzeToSilver_fp_matvieienko")
    .getOrCreate()
)

print(f"Spark version: {spark.version}")

BRONZE_BASE_PATH = "/tmp/bronze"
SILVER_BASE_PATH = "/tmp/silver"

tables = ["athlete_bio", "athlete_event_results"]


# Обробка кожної таблиці
for table in tables:
    print(f"\n=== Processing table: {table} (bronze -> silver) ===")

    bronze_path = os.path.join(BRONZE_BASE_PATH, table)
    silver_path = os.path.join(SILVER_BASE_PATH, table)

    # Читання parquet із bronze-шару
    df = spark.read.parquet(bronze_path)
    print(f"Loaded bronze table '{table}'. Row count: {df.count()}")

    # Очищення всіх текстових колонок
    for column in df.columns:
        if isinstance(df.schema[column].dataType, StringType):
            df = df.withColumn(column, clean_text_udf(col(column)))

    # Дедублікація рядків
    df = df.dropDuplicates()
    print(f"After dropDuplicates() for '{table}'. Row count: {df.count()}")

    # Запис у silver-шар у форматі Parquet
    os.makedirs(silver_path, exist_ok=True)
    (
        df.write
        .mode("overwrite")
        .parquet(silver_path)
    )
    print(f"Data for table '{table}' saved to {silver_path} in Parquet format.")

    # Перечитуємо silver і показуємо df.show() для логів / скріншотів
    silver_df = spark.read.parquet(silver_path)
    print(f"Sample data from silver/{table}:")
    silver_df.show(truncate=False)


# Завершення роботи Spark
spark.stop()
print("Spark session stopped.")
