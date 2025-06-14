import requests
from pyspark.sql import SparkSession

def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)
    if response.status_code == 200:
        with open(local_file_path + ".csv", "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {local_file_path}.csv")
    else:
        exit(f"Failed to download: {response.status_code}")

spark = SparkSession.builder.appName("landing_to_bronze").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    download_data(table)
    df = spark.read.option("header", True).csv(f"{table}.csv")
    df.write.mode("overwrite").parquet(f"bronze/{table}")