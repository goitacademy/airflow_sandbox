import requests
import sys
from pyspark.sql import SparkSession

def download_data(table_name: str):
    url = f"https://ftp.goit.study/neoversity/{table_name}.csv"
    print(f"Downloading from {url}")
    response = requests.get(url)
    if response.status_code == 200:
        local_file = f"dags/zyaremko_final_fp/datalake/landing/{table_name}.csv"
        with open(local_file, "wb") as f:
            f.write(response.content)
        print(f"Downloaded and saved {local_file}")
        return local_file
    else:
        sys.exit(f"Failed to download {url}, status {response.status_code}")

def main():
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    for table in ["athlete_bio", "athlete_event_results"]:
        local_csv = download_data(table)
        df = spark.read.option("header", True).csv(local_csv)

        df.show(5, truncate=False)

        output_path = f"dags/zyaremko_final_fp/datalake/bronze/{table}"
        df.write.mode("overwrite").parquet(output_path)
        print(f"Written bronze table: {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()



