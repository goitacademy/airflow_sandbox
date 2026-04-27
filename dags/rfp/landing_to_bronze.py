from __future__ import annotations

from pathlib import Path

import requests

from utils import get_spark_session, logger, write_parquet

FTP_BASE_URL = "https://ftp.goit.study/neoversity"
SOURCE_TABLES = ["athlete_bio", "athlete_event_results"]
LANDING_DIR = Path("landing")
BRONZE_DIR = Path("bronze")


def fetch_csv(table: str, dest_dir: Path) -> Path:
    """Download a CSV file from the FTP server and save it locally."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    file_path = dest_dir / f"{table}.csv"
    url = f"{FTP_BASE_URL}/{table}.csv"
    logger.info("Fetching %s", url)
    resp = requests.get(url, timeout=90)
    resp.raise_for_status()
    file_path.write_bytes(resp.content)
    logger.info("Saved to %s (%d bytes)", file_path, len(resp.content))
    return file_path


def ingest_table(spark, table: str) -> None:
    """Download one table, read as CSV and persist to the bronze layer."""
    # Stage 1: download CSV files from FTP to the landing zone.
    csv_file = fetch_csv(table, LANDING_DIR)

    # Stage 1: read CSV with Spark and write to bronze/{table} as Parquet.
    df = spark.read.option("header", True).option("inferSchema", True).csv(str(csv_file))
    df.show(20, truncate=False)

    output = BRONZE_DIR / table
    output.parent.mkdir(parents=True, exist_ok=True)
    write_parquet(df, output)


if __name__ == "__main__":
    spark = get_spark_session("goit-de-fp-oza-landing-to-bronze")
    try:
        for source_table in SOURCE_TABLES:
            ingest_table(spark, source_table)
    finally:
        spark.stop()