from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import re
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def clean_text(text):
    """
    Видаляє всі символи, окрім букв, цифр, коми, крапки та лапок.
    """
    if text is None:
        return None
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

# Реєструємо функцію clean_text() глобально, щоб використовувати в Spark
clean_text_udf = udf(clean_text, StringType())

def process_data(spark, table_name):
    """
    Читає таблицю у форматі Parquet з директорії Bronze 
    Чистить текст. 
    Робить дедублікацію рядків. 
    Зберігає в директорію Silver.
    """
    input_path = f"dags/oie_dags/bronze/{table_name}.parquet"
    output_path = f"dags/oie_dags/silver/{table_name}.parquet"

    try:
        # Читаємо Parquet файл
        df = spark.read.parquet(input_path)

        # Шукаємо текстові колонки та очищаємо їх
        for column_name, data_type in df.dtypes:
            if data_type == 'string':
                print(f" - Cleaning column: {column_name}")
                df = df.withColumn(column_name, clean_text_udf(col(column_name)))
        print("Text cleaning completed.")

        # Видаляємо дублікати
        print(f" - Number of rows before deduplication: {df.count()}")
        df = df.dropDuplicates()
        print(f" - Number of rows after deduplication: {df.count()}")
        print("Deduplication completed.")

        # Зберігаємо в директорію Silver
        if not os.path.exists("silver"):
            os.makedirs("silver")
            print("Created 'silver' directory.")

        df.write \
            .mode("overwrite") \
            .parquet(output_path)
            
        print(f"Successfully saved cleaned data to: {output_path}")
        
    except Exception as e:
        print(f"Error processing {table_name}: {e}")

def main():
        # Створюємо Spark сесію
    spark = SparkSession.builder \
        .appName("Bronze_to_Silver") \
        .config("spark.driver.memory", "2g") \
        .master("local[*]") \
        .getOrCreate()
    print("Spark session created.")     

    athlete_bio = "athlete_bio"
    athlete_event_results = "athlete_event_results"

    # Обробка athlete_bio таблиці
    print(f"--- Processing table: {athlete_bio} ---")
    process_data(spark, athlete_bio)

    # Обробка athlete_event_results таблиці
    print(f"--- Processing table: {athlete_event_results} ---")
    process_data(spark, athlete_event_results) 

    # Завершуємо Spark сесію
    spark.stop()
    print("Spark session stopped.")


if __name__ == "__main__":
    main()
