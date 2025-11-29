from pyspark.sql import SparkSession
import os
import requests


#  Функція для завантаження із ftp-сервера
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


# Створення SparkSession
spark = (
    SparkSession.builder
    .appName("LandingToBronze_fp_matvieienko")
    .getOrCreate()
)

print(f"Spark version: {spark.version}")

# Базовий шлях для bronze-шару
BRONZE_BASE_PATH = "/tmp/bronze"

# Таблиці, з якими працюємо
tables = ["athlete_bio", "athlete_event_results"]


# Обробка кожної таблиці
for table in tables:
    print(f"\n=== Processing table: {table} ===")

    # Завантаження CSV з FTP
    download_data(table)

    csv_path = f"{table}.csv"

    # Читання CSV у Spark DataFrame
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_path)
    )

    print(f"Loaded {table} from CSV. Row count: {df.count()}")

    # Шлях для збереження у bronze
    output_path = os.path.join(BRONZE_BASE_PATH, table)
    os.makedirs(output_path, exist_ok=True)

    # Запис у форматі Parquet
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Data for table '{table}' saved to {output_path} in Parquet format.")

    # Перечитуємо parquet і показуємо дані для логів (для скріншотів)
    bronze_df = spark.read.parquet(output_path)
    print(f"Sample data from bronze/{table}:")
    bronze_df.show(truncate=False)



# Завершення роботи Spark
spark.stop()
print("Spark session stopped.")
