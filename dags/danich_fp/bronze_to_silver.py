from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, trim
from pyspark.sql.types import StringType
import re

# --- КОНФІГУРАЦІЯ ---
TABLES = ["athlete_bio", "athlete_event_results"]
BRONZE_PATH = "bronze"
SILVER_PATH = "silver"


def clean_text(text):
    """Функція очищення тексту: видаляє символи, крім літер, цифр, коми, крапки, лапок."""
    if text is None:
        return None
    # Видалення всіх символів, окрім дозволених
    return re.sub(r"[^a-zA-Z0-9,.\"\']", "", str(text))


# Реєстрація UDF
clean_text_udf = udf(clean_text, StringType())


def run_bronze_to_silver():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for table_name in TABLES:
        print(f"--- Обробка таблиці: {table_name} ---")
        input_path = f"{BRONZE_PATH}/{table_name}"
        output_path = f"{SILVER_PATH}/{table_name}"

        # 1. Зчитування таблиці bronze
        try:
            df = spark.read.parquet(input_path)
        except Exception as e:
            print(f"Помилка читання з Bronze: {e}")
            continue

        # 2. Чистка тексту та обрізка пробілів
        df_cleaned = df
        string_cols = [
            f.name for f in df.schema.fields if isinstance(f.dataType, StringType)
        ]

        for col_name in string_cols:
            df_cleaned = df_cleaned.withColumn(col_name, clean_text_udf(col(col_name)))
            df_cleaned = df_cleaned.withColumn(col_name, trim(col(col_name)))

        # 3. Дедублікація рядків
        df_dedup = df_cleaned.dropDuplicates()

        # Фінальний DataFrame вивести на екран (для скріншота)
        print(f"DataFrame для {table_name} у Silver Zone (Перші 5 рядків):")
        df_dedup.show(5, truncate=False)  # <--- Скріншот 2/3 (для Part 2)

        # 4. Запис таблиці в папку silver/{table}
        df_dedup.write.mode("overwrite").parquet(output_path)
        print(f"Дані успішно збережено в Silver Zone: {output_path}")

    spark.stop()


if __name__ == "__main__":
    run_bronze_to_silver()
