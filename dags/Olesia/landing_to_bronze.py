import requests
from pyspark.sql import SparkSession
from pathlib import Path

# Cтворення Spark-сесії
spark = SparkSession.builder.appName("LandingToBronzeLayer").getOrCreate()

# Функція для завантаження CSV-файлів з FTP

def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"\n📥 Завантаження з: {downloading_url}")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        save_path = f"bronze/{local_file_path}.csv"
        with open(save_path, "wb") as file:
            file.write(response.content)
        print(f"✅ Файл збережено: {save_path}")
    else:
        print(f"❌ Не вдалося завантажити файл {local_file_path}. Код помилки: {response.status_code}")

# Основна логіка виконання

def main():
    Path("bronze").mkdir(parents=True, exist_ok=True)  # Створення каталогу, якщо не існує

    files = ["athlete_bio", "athlete_event_results"]
    for filename in files:
        download_data(filename)

    # Зчитування CSV у DataFrame
    for filename in files:
        csv_path = f"bronze/{filename}.csv"
        df = spark.read.option("header", True).csv(csv_path)
        print(f"\n🧾 Попередній перегляд {filename}:")
        df.show(5)
        print(f"🔢 Кількість рядків у {filename}: {df.count()}")

        # Збереження у Parquet
        df.write.mode("overwrite").parquet(f"bronze/{filename}")
        print(f"💾 Збережено як parquet: bronze/{filename}")

if __name__ == "__main__":
    main()


spark.stop()

