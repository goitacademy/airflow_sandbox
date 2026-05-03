"""landing_to_bronze.py — Stage 1 of the multi-hop datalake.

Downloads <table>.csv from the course FTP-over-HTTP, then has Spark read the CSV
and write it as parquet under bronze/<table>/.

Usage (also how it is invoked from the Airflow DAG):
    spark-submit landing_to_bronze.py <table>

Where <table> is one of: athlete_bio, athlete_event_results.
"""

from __future__ import annotations

import sys
from pathlib import Path

import requests
from pyspark.sql import SparkSession

FTP_BASE = "https://ftp.goit.study/neoversity/"


def download_csv(table: str, dest: Path) -> Path:
    url = f"{FTP_BASE}{table}.csv"
    print(f"downloading {url}")
    response = requests.get(url, timeout=120)
    if response.status_code != 200:
        sys.exit(f"failed to download {url}: HTTP {response.status_code}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(response.content)
    print(f"saved {dest} ({len(response.content):,} bytes)")
    return dest


def main(table: str) -> None:
    spark = (
        SparkSession.builder
        .appName(f"landing_to_bronze[{table}]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    csv_path = download_csv(table, Path(f"{table}.csv"))

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("escape", '"')
        .option("multiLine", "true")
        .csv(str(csv_path))
    )

    bronze_path = f"bronze/{table}"
    print(f"writing {df.count()} rows -> {bronze_path}")
    df.write.mode("overwrite").parquet(bronze_path)

    print(f"bronze/{table} schema:")
    df.printSchema()
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: landing_to_bronze.py <table>")
    main(sys.argv[1])
