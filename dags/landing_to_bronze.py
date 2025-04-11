import requests
from pyspark.sql import SparkSession

def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        with open(local_file_path + ".csv", 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}.csv")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")

def load_csv_to_bronze():
    spark = SparkSession.builder \
        .appName("Landing to Bronze") \
        .getOrCreate()

    # Download files
    download_data("athlete_bio")
    download_data("athlete_event_results")

    # Read CSV files
    athlete_bio_df = spark.read.csv("athlete_bio.csv", header=True, inferSchema=True)
    athlete_event_results_df = spark.read.csv("athlete_event_results.csv", header=True, inferSchema=True)

    # Save in Parquet format
    athlete_bio_df.write.mode("overwrite").parquet("bronze/athlete_bio")
    athlete_event_results_df.write.mode("overwrite").parquet("bronze/athlete_event_results")

    spark.stop()

if __name__ == "__main__":
    load_csv_to_bronze()
