from pyspark.sql import SparkSession
import requests
import os

def download_data(table_name: str):
    """Завантаження csv з FTP"""
    url = f"https://ftp.goit.study/neoversity/{table_name}.csv"
    local_path = f"landing/{table_name}.csv"
    os.makedirs("landing", exist_ok=True)
    print(f"⬇️ Завантажую {url}")
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(response.content)
        print(f"✅ {table_name}.csv збережено у {local_path}")
    else:
        raise Exception(f"❌ Помилка завантаження {table_name}, статус {response.status_code}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    for table in ["athlete_bio", "athlete_event_results"]:
        # Завантажуємо csv
        download_data(table)

        # Читаємо csv у Spark
        df = spark.read.option("header", True).csv(f"landing/{table}.csv")

        print(f"=== {table} sample data ===")
        df.show(5, truncate=False)

        # Зберігаємо у Parquet (bronze)
        output_path = f"bronze/{table}"
        df.write.mode("overwrite").parquet(output_path)
        print(f"✅ Збережено у {output_path}")
