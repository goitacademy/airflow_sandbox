import requests
from pyspark.sql import SparkSession
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
BRONZE_DIR = BASE_DIR / "bronze"

spark = SparkSession.builder.appName("LandingToBronzeLayer").getOrCreate()


def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading: {downloading_url}")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        save_path = BRONZE_DIR / f"{local_file_path}.csv"
        with open(save_path, "wb") as file:
            file.write(response.content)
        print(f"Saved: {save_path}")
    else:
        print(f"Failed: {local_file_path} (Code: {response.status_code})")


def main():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)  # Create folder

    files = ["athlete_bio", "athlete_event_results"]

    for filename in files:
        download_data(filename)


    for filename in files:
        csv_path = BRONZE_DIR / f"{filename}.csv"
        df = spark.read.option("header", True).csv(str(csv_path))
        print(f"Preview {filename}:")
        df.show(3)
        print(f"Rows: {df.count()}")

        df.write.mode("overwrite").parquet(str(BRONZE_DIR / filename))
        print(f"Parquet saved: {BRONZE_DIR / filename}")

if __name__ == "__main__":
    main()

spark.stop()
