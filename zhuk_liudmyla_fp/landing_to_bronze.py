"""
Part 2 · landing_to_bronze
==========================

1) Завантажує CSV з FTP-сервера GoIT (https://ftp.goit.study/neoversity/)
   через SparkContext.addFile — Spark сам дистрибутує файл на всі executors,
   що гарантує видимість із будь-якого вузла кластера.
2) Читає CSV за допомогою Spark.
3) Зберігає результат у parquet у папку bronze/<table>/.

Запуск:
    spark-submit landing_to_bronze.py                     # обидві таблиці
    spark-submit landing_to_bronze.py athlete_bio         # лише одна
"""

from __future__ import annotations

import os
import sys

import requests
from pyspark import SparkFiles
from pyspark.sql import SparkSession

FTP_BASE_URL = "https://ftp.goit.study/neoversity/"
DEFAULT_TABLES = ["athlete_bio", "athlete_event_results"]


def download_data(local_file_path: str) -> str:
    """Завантажити <table>.csv з FTP у локальну директорію драйвера (для логів)."""
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
    # 1) Завантажимо CSV на driver і залогуємо (як у шаблоні курсу).
    download_data(table)

    # 2) І додатково розшлемо через Spark на всі executors — щоб гарантовано
    #    читався з будь-якого вузла кластера (spark-cluster + worker можуть бути
    #    на різних хостах, тому покладатись на локальний шлях драйвера не можна).
    url = FTP_BASE_URL + table + ".csv"
    spark.sparkContext.addFile(url)
    distributed_path = "file://" + SparkFiles.get(f"{table}.csv")
    print(f"[{table}] Spark distributed file: {distributed_path}")

    # 3) Spark читає CSV → DataFrame. Без inferSchema — швидше і менше шансів
    #    на впав на executor-і (всі колонки як string, що нас повністю влаштовує
    #    для bronze-шару; типи приведемо у silver/gold).
    df = (
        spark.read
        .option("header", "true")
        .csv(distributed_path)
    )
    print(f"[{table}] {df.count()} rows read from CSV")
    df.printSchema()
    df.show(5, truncate=False)

    # 4) DataFrame → Parquet у bronze/<table>/
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
