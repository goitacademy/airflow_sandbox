import sys
import requests

from pyspark.sql import SparkSession


def download_data(table_name: str) -> str:
    base_url = "https://ftp.goit.study/neoversity/"
    file_name = f"{table_name}.csv"
    download_url = base_url + file_name

    print(f"Downloading from: {download_url}")

    response = requests.get(download_url)

    if response.status_code != 200:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")

    with open(file_name, "wb") as file:
        file.write(response.content)

    print(f"Downloaded successfully: {file_name}")
    return file_name


def main(table_name: str):
    print(f"Starting landing_to_bronze for table: {table_name}")

    spark = (
        SparkSession.builder
        .appName(f"LandingToBronze_{table_name}")
        .getOrCreate()
    )

    local_csv_path = download_data(table_name)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(local_csv_path)
    )

    print("Schema:")
    df.printSchema()

    print("Preview:")
    df.show(10, truncate=False)

    output_path = f"bronze/{table_name}"

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Saved to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("Table name is required. Example: python landing_to_bronze.py athlete_bio")

    main(sys.argv[1])