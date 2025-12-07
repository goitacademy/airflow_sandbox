from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
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


def csv_to_parquet_with_spark(spark: SparkSession, csv_path: str, parquet_path: str):
    try:
        # Read CSV file
        df = spark.read.csv(
            csv_path,
            header=True,
            inferSchema=True,
            sep=","
        )
        df.show(20)
        # Write data in Parquet format
        df.write.mode("overwrite").parquet(parquet_path)
        print(f"Data successfully saved in Parquet format by path: {parquet_path}")
        return True

    except AnalysisException as e:
        print(f"Fail Spark Analysis: {e}")
        return False
    except Exception as e:
        print(f"Unknown error Spark: {e}")
        return False

if __name__ == "__main__":
    print("Download data")
    download_data("athlete_bio")
    download_data("athlete_event_results")

    # Init Spark Session
    spark = SparkSession.builder \
        .appName("LandingToBronzePipeline") \
        .getOrCreate()

    try:
        print("Bronze (change to Parquet format)")
        csv_to_parquet_with_spark(spark, "athlete_bio.csv", "bronze/athlete_bio.parquet")
        csv_to_parquet_with_spark(spark, "athlete_event_results.csv", "bronze/athlete_event_results.parquet")

    finally:
        # Stop Spark Session
        spark.stop()
        print("Spark Session stopped.")