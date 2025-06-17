import requests
from pyspark.sql import SparkSession

def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        with open(local_file_path + ".csv", 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}.csv")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")

def main():
    # Назва файлу (без розширення)
    file_name = "athlete_bio"  # або "athlete_event_results" для другого файлу

    # Завантаження CSV з ftp
    download_data(file_name)

    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    # Читання CSV в Spark DataFrame
    df = spark.read.option("header", True).csv(file_name + ".csv")

    # Запис у parquet у папку bronze
    df.write.mode("overwrite").parquet(f"data/bronze/{file_name}")

    spark.stop()

if __name__ == "__main__":
    main()
