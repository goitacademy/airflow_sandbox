
from pyspark.sql import SparkSession
import os
import requests

def download_data(file):
    url = f"https://ftp.goit.study/neoversity/{file}.csv"
    print(f"Downloading from {url}")
    response = requests.get(url, timeout=30)

    if response.status_code == 200:
        with open(f"{file}.csv", 'wb') as f:
            f.write(response.content)
        print(f"File {file}.csv downloaded successfully.")
    else:
        raise Exception(f"Failed to download {file}. Status code: {response.status_code}")

def process_table(table):
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    local_path = f"{table}.csv"
    output_path = f"/tmp/bronze/{table}"

    df = spark.read.csv(local_path, header=True, inferSchema=True)
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)

    print(f"Data saved to {output_path}")
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

    spark.stop()

def main():
    tables = ["athlete_bio", "athlete_event_results"]
    for table in tables:
        download_data(table)
        process_table(table)

if __name__ == "__main__":
    main()
