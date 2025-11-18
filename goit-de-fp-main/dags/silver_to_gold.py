# Імпортуємо необхідні модулі з PySpark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import avg, current_timestamp 
import os  

# Створюємо сесію Spark з ім'ям програми та підключенням MySQL драйвера
spark = (
    SparkSession.builder
    .appName("SilverToGold")
    .config("spark.jars", "/opt/airflow/dags/goit-de-fp-main/mysql-connector-j-8.0.32.jar")
    .getOrCreate()
)

# Читаємо дані з паркетних файлів для таблиць "athlete_bio" та "athlete_event_results"
athlete_bio_df = spark.read.parquet("/tmp/silver/athlete_bio") 
athlete_event_results_df = spark.read.parquet("/tmp/silver/athlete_event_results") 

# Перейменовуємо стовпець "country_noc" на "bio_country_noc" у таблиці про атлетів
athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

# Виконуємо об'єднання двох DataFrame за стовпцем "athlete_id"
joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

# Агрегуємо дані: обчислюємо середні значення для зросту та ваги, додаємо поточний час
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),  
    avg("weight").alias("avg_weight"),  
    current_timestamp().alias("timestamp"),  
)

# Визначаємо шлях для збереження результатів у форматі Parquet
output_path = "/tmp/gold/avg_stats"
# Створюємо директорію, якщо її ще не існує
os.makedirs(output_path, exist_ok=True)

# Записуємо агреговані дані у форматі Parquet, перезаписуючи існуючі файли
aggregated_df.write.mode("overwrite").parquet(output_path)

# Виводимо повідомлення про успішне збереження даних
print(f"Data saved to {output_path}")

# Читаємо збережені дані і виводимо їх
df = spark.read.parquet(output_path)
df.show(truncate=False)

# ==================== ДОДАНО: ЗАПИС В MySQL ====================

# Налаштування для підключення до MySQL (ВИПРАВЛЕНО: база olympic_dataset)
jdbc_config = {
    "url": "jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "driver": "com.mysql.cj.jdbc.Driver",
}

print("Writing data to MySQL table: serhii_kravchenko_enriched_athlete_avg")

try:
    # Запис агрегованих даних в MySQL
    aggregated_df.write.format("jdbc").options(
        url=jdbc_config["url"],
        driver=jdbc_config["driver"],
        dbtable="serhii_kravchenko_enriched_athlete_avg",
        user=jdbc_config["user"],
        password=jdbc_config["password"],
    ).mode("overwrite").save()
    
    print("✅ Data successfully saved to MySQL table: serhii_kravchenko_enriched_athlete_avg")
    print(f"✅ Total records written: {aggregated_df.count()}")
    
except Exception as e:
    print(f"❌ Error writing to MySQL: {e}")
    raise

# Завершуємо роботу з сесією Spark
spark.stop()