"""
Part 2 · bronze_to_silver
=========================

1) Зчитує parquet-таблиці з bronze/<table>/.
2) Чистить текстові колонки (залишає тільки літери/цифри та базові пунктуацію).
3) Дедублікує рядки.
4) Зберігає parquet у silver/<table>/.

Для очистки тексту використано нативний Spark `regexp_replace` замість
Python UDF — Python UDF вимагає pickle→send→unpickle на executor-ах і на
shared кластері курсу спричиняє крах Python worker-ів (executors exit 1).
Регекс тотожний рекомендованому: залишає a-zA-Z0-9 , . \\ " '.

Запуск:
    spark-submit bronze_to_silver.py athlete_bio athlete_event_results
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, regexp_replace

DEFAULT_TABLES = ["athlete_bio", "athlete_event_results"]

# Регекс залишає: літери a-z/A-Z, цифри 0-9, кома, крапка, зворотна коса,
# подвійна й одинарна лапки. Усе інше — викидає.
# У Java-regexp char class '\\' означає літеральну \, тож '\\\\' у Python
# рядку = '\\' у регексі = літеральна коса риска.
CLEAN_TEXT_PATTERN = r"[^a-zA-Z0-9,.\\\"']"


def clean_string_columns(df: DataFrame) -> DataFrame:
    string_cols = [
        f.name for f in df.schema.fields
        if f.dataType.simpleString() == "string"
    ]
    for col_name in string_cols:
        df = df.withColumn(
            col_name,
            regexp_replace(col(col_name), CLEAN_TEXT_PATTERN, ""),
        )
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
