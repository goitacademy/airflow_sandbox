from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import requests  # <--- ПЕРЕМІЩЕНО ВГОРУ
import os

# --- КОНФІГУРАЦІЯ ---
TABLES = ["athlete_bio", "athlete_event_results"]
FTP_BASE_URL = "https://ftp.goit.study/neoversity/"
BRONZE_PATH = "bronze"  # Каталог для збереження даних


def download_data(table_name):
    """Завантажує CSV-файл із FTP-сервера."""
    local_file_path = f"{table_name}.csv"
    downloading_url = f"{FTP_BASE_URL}{local_file_path}"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        # Записуємо у локальний файл
        with open(local_file_path, "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        # Важливо викликати виняток, щоб Airflow помітив помилку
        raise Exception(
            f"Failed to download {table_name}. Status code: {response.status_code}"
        )

    return local_file_path


def run_landing_to_bronze():
    # Створення Spark сесії
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for table_name in TABLES:
        print(f"--- Обробка таблиці: {table_name} ---")

        # 1. Завантаження CSV-файлу з FTP
        try:
            local_csv_path = download_data(table_name)
        except Exception as e:
            print(e)
            continue

        # 2. Читання CSV за допомогою Spark
        df = spark.read.csv(local_csv_path, header=True, inferSchema=True)

        # Фінальний DataFrame вивести на екран (для скріншота)
        print(f"DataFrame для {table_name} у Landing Zone (Перші 5 рядків):")
        df.show(5, truncate=False)  # <--- Скріншот 1/3 (для Part 2)

        # 3. Збереження у форматі parquet
        output_path = f"{BRONZE_PATH}/{table_name}"
        df.write.mode("overwrite").parquet(output_path)
        print(f"Дані успішно збережено в Bronze Zone: {output_path}")

        # Прибирання локального CSV-файлу
        os.remove(local_csv_path)

    spark.stop()


if __name__ == "__main__":
    # Видалено блок встановлення PIP, щоб Airflow не падав під час парсингу
    run_landing_to_bronze()
