import os
import requests
from pyspark.sql import SparkSession

# Автоматичне визначення поточної папки
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")

spark = SparkSession.builder.appName("LandingToBronzeLayer").getOrCreate()

def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading: {downloading_url}")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        # Зберігаємо тимчасовий CSV файл у поточну папку
        save_path = os.path.join(BASE_DIR, f"{local_file_path}.csv")
        with open(save_path, "wb") as file:
            file.write(response.content)
        print(f"Saved original CSV: {save_path}")
    else:
        print(f"Failed: {local_file_path} (Code: {response.status_code})")

def main():
    # Безпечно створюємо папку bronze
    os.makedirs(BRONZE_DIR, exist_ok=True)

    files = ["athlete_bio", "athlete_event_results"]
    
    for filename in files:
        download_data(filename)

    for filename in files:
        csv_path = os.path.join(BASE_DIR, f"{filename}.csv")
        df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
        
        print(f"Preview {filename}:")
        df.show(3)

        # Запис у форматі Parquet в папку bronze
        parquet_path = os.path.join(BRONZE_DIR, filename)
        df.write.mode("overwrite").parquet(parquet_path)
        print(f"Parquet saved: {parquet_path}")
        
        # Видаляємо тимчасовий CSV файл, щоб не забивати диск
        if os.path.exists(csv_path):
            os.remove(csv_path)

if __name__ == "__main__":
    main()
    spark.stop()