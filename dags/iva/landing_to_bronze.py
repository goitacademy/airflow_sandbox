from pyspark.sql import SparkSession
import requests
import sys
import os


def download_data(table_name: str) -> str:
    """
    Завантажує CSV з FTP і зберігає в data_lake/landing/{table}.csv
    """
    base_url = "https://ftp.goit.study/neoversity/"
    downloading_url = base_url + table_name + ".csv"
    print(f"Downloading from {downloading_url}")

    response = requests.get(downloading_url)
    if response.status_code == 200:
        os.makedirs("data_lake/landing", exist_ok=True)
        local_path = os.path.join("data_lake", "landing", table_name + ".csv")
        with open(local_path, "wb") as f:
            f.write(response.content)
        print(f"File downloaded successfully and saved as {local_path}")
        return local_path
    else:
        raise SystemExit(f"Failed to download the file. Status code: {response.status_code}")


def run_etl(table_name: str) -> None:
    """
    Landing -> Bronze:
    - читаємо CSV
    - зберігаємо як Parquet у data_lake/bronze/{table}
    """
    spark = (
        SparkSession.builder
        .appName(f"LandingToBronze_{table_name}")
        .getOrCreate()
    )

    csv_path = download_data(table_name)

    df = (
        spark.read
        .option("header", "true")
        .csv(csv_path)
    )

    # df.show() для скріншота в логах
    df.show(20, truncate=False)

    output_path = os.path.join("data_lake", "bronze", table_name)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.write.mode("overwrite").parquet(output_path)
    print(f"Bronze table saved to {output_path}")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: landing_to_bronze.py <table_name>")
    table = sys.argv[1]  # athlete_bio або athlete_event_results
    run_etl(table)
