import os
import sys
import requests

from pyspark.sql import SparkSession


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def download_data(table_name: str) -> str:
    base_url = "https://ftp.goit.study/neoversity/"
    file_name = f"{table_name}.csv"
    download_url = base_url + file_name
    local_file_path = os.path.join(BASE_DIR, file_name)

    response = requests.get(download_url)

    if response.status_code != 200:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")

    with open(local_file_path, "wb") as file:
        file.write(response.content)

    return local_file_path


def main(table_name: str):
    spark = (
        SparkSession.builder
        .appName(f"LandingToBronze_{table_name}")
        .getOrCreate()
    )

    # Етап 1: завантаження CSV з FTP
    local_csv_path = download_data(table_name)

    # Етап 2: читання CSV через Spark
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(local_csv_path)
    )

    # Етап 3: запис у bronze layer у форматі parquet
    output_path = os.path.join(BASE_DIR, "bronze", table_name)

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Table {table_name} saved to bronze layer")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception(
            "Table name is required. Example: python landing_to_bronze.py athlete_bio"
        )

    main(sys.argv[1])