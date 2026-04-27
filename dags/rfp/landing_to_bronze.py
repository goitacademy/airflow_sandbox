import os
import requests
from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Конфігурація
# ---------------------------------------------------------------------------

BASE_URL = "https://ftp.goit.study/neoversity/"
TABLES = ["athlete_bio", "athlete_event_results"]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LANDING_DIR = os.path.join(BASE_DIR, "landing")
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")


# ---------------------------------------------------------------------------
# Завантаження даних з FTP
# ---------------------------------------------------------------------------

def download_data(table_name):
    """Завантажує CSV-файл з FTP-сервера."""
    url = f"{BASE_URL}{table_name}.csv"
    local_path = os.path.join(LANDING_DIR, f"{table_name}.csv")

    os.makedirs(LANDING_DIR, exist_ok=True)

    print(f"Downloading from {url}")
    response = requests.get(url)

    if response.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(response.content)
        print(f"File downloaded successfully and saved as {local_path}")
    else:
        raise RuntimeError(
            f"Failed to download the file. Status code: {response.status_code}"
        )

    return local_path


# ---------------------------------------------------------------------------
# Головна логіка
# ---------------------------------------------------------------------------

def main():
    # Створення Spark сесії
    spark = (
        SparkSession.builder
        .appName("LandingToBronze")
        .getOrCreate()
    )

    for table in TABLES:
        print("=" * 60)
        print(f"Processing table: {table}")
        print("=" * 60)

        # Завантажуємо CSV з FTP-сервера
        csv_path = download_data(table)

        # Spark читає CSV-файл
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        print(f"{table}: {df.count()} rows loaded from CSV")
        df.show(truncate=False)

        # Зберігаємо у форматі parquet у папку bronze/{table}
        bronze_path = os.path.join(BRONZE_DIR, table)
        df.write.parquet(bronze_path, mode="overwrite")
        print(f"{table}: saved to {bronze_path}")

    spark.stop()
    print("\nLanding to Bronze — завершено!")


if __name__ == "__main__":
    main()