"""
Part 2 · landing_to_bronze
==========================

1) Завантажує CSV з FTP-сервера GoIT (https://ftp.goit.study/neoversity/).
2) Читає CSV за допомогою Spark.
3) Зберігає результат у parquet у папку bronze/<table>/.

Запускається двічі — для таблиць athlete_bio та athlete_event_results.
Таблиці, які треба обробити, передаються як CLI-аргументи:

    spark-submit landing_to_bronze.py athlete_bio athlete_event_results

Якщо CLI-аргументи не задані — використовуються обидві таблиці за замовчуванням.
"""

from __future__ import annotations

import os
import sys

import requests
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Конфіг
# ---------------------------------------------------------------------------
FTP_BASE_URL = "https://ftp.goit.study/neoversity/"
DEFAULT_TABLES = ["athlete_bio", "athlete_event_results"]


def download_data(local_file_path: str) -> str:
    """Завантажити <table>.csv з FTP-сервера у поточну директорію. Повертає локальний шлях."""
    downloading_url = FTP_BASE_URL + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url, timeout=120)
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to download {downloading_url}. Status code: {response.status_code}"
        )
    out_path = local_file_path + ".csv"
    with open(out_path, "wb") as f:
        f.write(response.content)
    print(f"File downloaded successfully and saved as {out_path}")
    return out_path


def process_table(spark: SparkSession, table: str) -> None:
    csv_path = download_data(table)

    # CSV → DataFrame
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_path)
    )
    print(f"[{table}] {df.count()} rows read from CSV")
    df.printSchema()
    df.show(5, truncate=False)

    # DataFrame → Parquet у bronze/<table>
    bronze_path = os.path.join("bronze", table)
    df.write.mode("overwrite").parquet(bronze_path)
    print(f"[{table}] saved to {bronze_path}")


def main(tables: list[str]) -> None:
    spark = (
        SparkSession.builder
        .appName("LandingToBronze")
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
