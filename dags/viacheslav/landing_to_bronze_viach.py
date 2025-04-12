import requests
from pyspark.sql import SparkSession
import os
import sys

def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the local file in write-binary mode and write the content of the response to it
        with open(local_file_path + ".csv", 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}.csv")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Landing to Bronze") \
        .getOrCreate()

    # Tables to process
    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        # Download the data
        download_data(table)

        # Read the CSV file
        df = spark.read.csv(f"{table}.csv", header=True, inferSchema=True)

        # Create bronze directory if it doesn't exist
        bronze_path = f"bronze/{table}"
        os.makedirs(os.path.dirname(bronze_path), exist_ok=True)

        # Save as Parquet
        df.write.mode("overwrite").parquet(bronze_path)

        # Show DataFrame
        print(f"Saved {table} to bronze layer:")
        df.show()

    # Clean up downloaded CSV files
    for table in tables:
        if os.path.exists(f"{table}.csv"):
            os.remove(f"{table}.csv")
            print(f"Removed temporary file {table}.csv")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()