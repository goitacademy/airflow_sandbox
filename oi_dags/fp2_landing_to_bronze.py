# Написати файл landing_to_bronze.py. Він має:
# завантажувати таблиці athlete_bio та athlete_event_results
# з ftp-сервера  https://ftp.goit.study/neoversity/athlete_bio.csv і
# https://ftp.goit.study/neoversity/athlete_event_results.csvв оригінальному форматі csv,
# за допомогою Spark прочитати csv-файл і зберегти його у форматі parquet.
# зберегти його у форматі parquet у папку bronze/{table}, де {table} — ім’я таблиці

from pyspark.sql import SparkSession
import requests

# Для завантаження із ftp-сервера варто використати функцію
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
#---------MAIN-----------------------------------------------
spark = (
    SparkSession.builder
    .appName("LandingToBronze")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    # 1. Download CSV locally
    download_data(table)

    local_csv_path = table + ".csv"

    # 2. Read CSV with Spark
    print(f"Reading CSV with Spark: {local_csv_path}")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(local_csv_path)
    )

    # 3. Save to bronze/{table}
    bronze_path = f"bronze/{table}"
    print(f"Writing Parquet → {bronze_path}")

    df.printSchema()
    df.show(5)

    (
        df.write
        .mode("overwrite")
        .parquet(bronze_path)
    )

spark.stop()
print("Done")







