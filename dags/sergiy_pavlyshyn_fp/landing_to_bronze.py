import os
import requests
from pyspark.sql import SparkSession


def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Завантажуємо з {downloading_url}...")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        with open(local_file_path + ".csv", "wb") as file:
            file.write(response.content)
        print(f"Файл успішно завантажено та збережено як {local_file_path}.csv")
    else:
        exit(f"Не вдалося завантажити файл. Status code: {response.status_code}")


def main():
    # 1. Завантажуємо файли з FTP
    tables = ["athlete_bio", "athlete_event_results"]
    for table in tables:
        download_data(table)

    spark = (
        SparkSession.builder.appName("Landing_to_Bronze")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    for table in tables:
        print(f"\nОбробка таблиці {table} (CSV -> Parquet)...")

        df = spark.read.csv(f"{table}.csv", header=True, inferSchema=True)

        output_path = f"bronze/{table}"

        df.write.mode("overwrite").parquet(output_path)

        print(f"Таблиця {table} успішно збережена у {output_path}")

        print("Перші 5 рядків (для звіту):")
        df.show(5)

    spark.stop()
    print("\nПроцес Landing to Bronze завершено успішно!")


if __name__ == "__main__":
    main()
