"""
Фінальний проєкт — Частина 2, Крок 2.
Bronze to Silver.

Зчитує parquet-таблиці з bronze/, очищає текстові колонки,
дедублікує рядки, записує у silver/{table}.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ---------------------------------------------------------------------------
# Конфігурація
# ---------------------------------------------------------------------------

TABLES = ["athlete_bio", "athlete_event_results"]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "silver")


import re
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# ---------------------------------------------------------------------------
# Функція очищення тексту (через UDF, як вимагається у завданні)
# ---------------------------------------------------------------------------

def clean_text(text):
    """Видаляє всі символи, крім a-zA-Z0-9,.\"' """
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text)) if text is not None else None

clean_text_udf = udf(clean_text, StringType())

def clean_text_column(df, col_name):
    return df.withColumn(col_name, clean_text_udf(col(col_name)))


# ---------------------------------------------------------------------------
# Головна логіка
# ---------------------------------------------------------------------------

def main():
    # Створення Spark сесії
    spark = (
        SparkSession.builder
        .appName("BronzeToSilver")
        .getOrCreate()
    )

    for table in TABLES:
        print("=" * 60)
        print(f"Processing table: {table}")
        print("=" * 60)

        # Зчитуємо parquet з bronze/
        bronze_path = os.path.join(BRONZE_DIR, table)
        df = spark.read.parquet(bronze_path)
        print(f"{table} (bronze): {df.count()} rows")

        # Очищаємо текстові колонки
        for col_name, col_type in df.dtypes:
            if col_type == "string":
                df = clean_text_column(df, col_name)

        # Дедублікація рядків
        df = df.dropDuplicates()
        print(f"{table} (після очищення + дедублікації): {df.count()} rows")
        df.show(truncate=False)

        # Записуємо у silver/{table}
        silver_path = os.path.join(SILVER_DIR, table)
        df.write.parquet(silver_path, mode="overwrite")
        print(f"{table}: saved to {silver_path}")

    spark.stop()
    print("\nBronze to Silver — завершено!")


if __name__ == "__main__":
    main()
