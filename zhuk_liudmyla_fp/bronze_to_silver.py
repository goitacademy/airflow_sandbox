"""
Part 2 · bronze_to_silver
=========================

1) Зчитує parquet-таблиці з bronze/<table>/.
2) Чистить текстові колонки (залишає тільки літери/цифри та базові пункт.).
3) Дедублікує рядки.
4) Зберігає parquet у silver/<table>/.

Запуск:
    spark-submit bronze_to_silver.py athlete_bio athlete_event_results

За замовчуванням обробляє обидві таблиці.
"""

from __future__ import annotations

import os
import re
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

DEFAULT_TABLES = ["athlete_bio", "athlete_event_results"]


def clean_text(text):
    if text is None:
        return None
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', "", str(text))


clean_text_udf = udf(clean_text, StringType())


def clean_string_columns(df: DataFrame) -> DataFrame:
    """Застосувати clean_text_udf до всіх string-колонок DataFrame."""
    string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
    for col_name in string_cols:
        df = df.withColumn(col_name, clean_text_udf(col(col_name)))
    return df


def process_table(spark: SparkSession, table: str) -> None:
    bronze_path = os.path.join("bronze", table)
    silver_path = os.path.join("silver", table)

    df = spark.read.parquet(bronze_path)
    print(f"[{table}] {df.count()} rows read from {bronze_path}")

    df = clean_string_columns(df)
    df = df.dropDuplicates()
    print(f"[{table}] {df.count()} rows after cleaning + deduplication")

    df.show(5, truncate=False)

    df.write.mode("overwrite").parquet(silver_path)
    print(f"[{table}] saved to {silver_path}")


def main(tables: list[str]) -> None:
    spark = (
        SparkSession.builder
        .appName("BronzeToSilver")
        .getOrCreate()
    )
    try:
        for table in tables:
            process_table(spark, table)
    finally:
        spark.stop()


if __name__ == "__main__":
    tables = sys.argv[1:] or DEFAULT_TABLES
    main(tables)
