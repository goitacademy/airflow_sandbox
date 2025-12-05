from pyspark.sql import SparkSession
import requests
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def download_data(local_file_path):

    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        
        output_dir = "dags/oie_dags"
        output_path = f"{output_dir}/{local_file_path}.csv"
        
        os.makedirs(output_dir, exist_ok=True)

        # Open the local file in write-binary mode and write the content of the response to it
        with open(output_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {output_path}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")


def process_csv_to_parquet(spark, file_name, table_name):

    file_name = file_name + ".csv"
    table_name = table_name + ".parquet"
    
    if not os.path.exists("bronze"):
        os.makedirs("bronze")   
        print("Created 'bronze' directory.")
    
    input_path = f"dags/oie_dags/{file_name}"
    output_path = f"dags/oie_dags/bronze/{table_name}"
    
    try:
        # Завантажуємо CSV файл у DataFrame
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", ",") \
            .csv(input_path)

        print("First 5 rows of the DataFrame:")
        df.show(5)

        # Записуємо у формат Parquet
        df.write \
            .mode("overwrite") \
            .parquet(output_path)
        print(f"File successfully converted to Parquet: {output_path}")
    
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    # Створюємо Spark сесію
    spark = SparkSession.builder \
        .appName("CSV_to_Parquet") \
        .config("spark.driver.memory", "2g") \
        .master("local[*]") \
        .getOrCreate()
    print("Spark session created.")

    athlete_bio = "athlete_bio"
    athlete_event_results = "athlete_event_results"

    # Завантаження athlete_bio файлу та конвертація у Parquet
    print(f"--- Processing table: {athlete_bio} ---")
    download_data(athlete_bio)
    process_csv_to_parquet(spark,athlete_bio, athlete_bio)

    # Завантаження athlete_event_results файлу та конвертація у Parquet
    print(f"--- Processing table: {athlete_event_results} ---") 
    download_data(athlete_event_results)
    process_csv_to_parquet(spark, athlete_event_results, athlete_event_results)

    # Завершуємо сесію Spark
    spark.stop()
    print("Spark session stopped.")

if __name__ == "__main__":
    main()

