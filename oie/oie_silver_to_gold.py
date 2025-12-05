import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import avg, current_timestamp, col
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def aggregated_data(spark, first_table_name, second_table_name):
    """
    Читає таблицю у форматі Parquet з директорії Silver
    Робить join за колонкою athlete_id.
    Для кожної комбінації цих 4 стовпчиків — sport, medal, sex, country_noc — знаходити середні значення weight і height.
    Додає колонку timestamp з часовою міткою виконання програми.
    Зберігає результат у форматі Parquet в gold/avg_stats.
    """
    input_path_first_table = f"dags/oie_dags/silver/{first_table_name}.parquet"
    input_path_second_table = f"dags/oie_dags/silver/{second_table_name}.parquet"
    output_path = f"dags/oie_dags/gold/avg_stats.parquet"

    try:
        # Читаємо Parquet файли
        df_first = spark.read.parquet(input_path_first_table)
        df_second = spark.read.parquet(input_path_second_table)
        print(f"Successfully read tables: {first_table_name}, {second_table_name}") 

        # Очищаємо дані, видаливши записи з відсутніми або некоректними значеннями height та weight
        df_first = df_first \
            .withColumn("height", col("height").cast("double")) \
            .withColumn("weight", col("weight").cast("double")) \
            .na.drop(subset=["height", "weight"]) \
            .drop("country_noc") # Видалення стовпця country_noc, оскільки він дублює інформацію, що вже є в іншій таблиці
        print("Data cleaning completed.")   

        # Виконуємо об'єднання таблиць за спільним ключем 'athlete_id'
        joined_df = df_first.join(df_second, on="athlete_id", how="inner")
        print("Tables successfully joined on 'athlete_id'.")

        # Обчислюємо середні значення height та weight для кожної комбінації sport, medal, sex, country_noc
        aggregated_df = joined_df \
            .groupBy("sport", "medal", "sex", "country_noc") \
            .agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight")
            ) \
            .withColumn("calculation_timestamp", current_timestamp())
        print("Aggregation completed.")

        # Зберігаємо в директорію Silver
        if not os.path.exists("gold"):
            os.makedirs("gold")
            print("Created 'gold' directory.")

        aggregated_df.write \
            .mode("overwrite") \
            .parquet(output_path)
            
        print(f"Successfully saved cleaned data to: {output_path}")
        
    except Exception as e:
        print(f"Error processing data: {e}")

def main():
    # Створюємо Spark сесію
    spark = SparkSession.builder \
        .appName("Silver_to_Gold") \
        .config("spark.driver.memory", "2g") \
        .master("local[*]") \
        .getOrCreate()
    print("Spark session created.")

    athlete_bio = "athlete_bio"
    athlete_event_results = "athlete_event_results" 

    print(f"--- Aggregating tables: {athlete_bio}, {athlete_event_results} ---")
    aggregated_data(spark, athlete_bio, athlete_event_results)

    spark.stop()    
    print("Spark session stopped.")

if __name__ == "__main__":
    main()
