from pyspark.sql import SparkSession
import os
import requests


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
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")


def process_table(table):
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()
    local_path = f"{table}.csv"
    output_path = f"bronze/{table}"

    # read csv
    df = spark.read.csv(local_path, header=True, inferSchema=True)

    # create output directory
    os.makedirs(output_path, exist_ok=True)

    # save table in parquet format
    df.write.mode("overwrite").parquet(output_path)
    print(f"Successfully processed {table} and saved as {output_path}")

    # show final dataframe for the job
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

    spark.stop()

tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    download_data(table)
    process_table(table)