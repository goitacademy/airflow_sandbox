from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, when, trim, expr
from pyspark.sql.types import FloatType

# --- КОНФІГУРАЦІЯ ---
SILVER_PATH = "silver"
GOLD_PATH = "gold/avg_stats"


def run_silver_to_gold():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Зчитування двох таблиць із silver layer
    input_bio = f"{SILVER_PATH}/athlete_bio"
    input_results = f"{SILVER_PATH}/athlete_event_results"

    try:
        bio_df = spark.read.parquet(input_bio)
        results_df = spark.read.parquet(input_results)
    except Exception as e:
        print(f"Помилка читання з Silver: {e}")
        spark.stop()
        return

    # Підготовка bio_df: приведення height/weight до числового типу
    # Використовуємо expr для безпечного приведення, оскільки дані можуть бути змішані
    bio_df_cleaned = (
        bio_df.withColumn("height", expr("try_cast(height as float)"))
        .withColumn("weight", expr("try_cast(weight as float)"))
        .filter(col("height").isNotNull() & col("weight").isNotNull())
        .select("athlete_id", "sex", "height", "weight")
    )

    # 2. Об’єднання (join) за колонкою athlete_id
    joined_df = results_df.join(bio_df_cleaned, on="athlete_id", how="inner")

    # Підготовка: Заміна порожніх значень медалей на "No Medal"
    agg_ready_df = joined_df.withColumn(
        "medal",
        when(
            trim(col("medal")).isNull() | (trim(col("medal")) == ""), "No Medal"
        ).otherwise(col("medal")),
    )

    # 3. Групування та агрегація (середні значення weight і height)
    group_cols = ["sport", "medal", "sex", "country_noc"]

    final_agg_df = (
        agg_ready_df.groupBy(group_cols)
        .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight"))
        .withColumn("timestamp", current_timestamp())
    )  # 4. Додавання колонки timestamp

    # Фінальний DataFrame вивести на екран (для скріншота)
    print(f"Фінальний DataFrame у Gold Zone (Перші 5 рядків):")
    final_agg_df.show(5, truncate=False)  # <--- Скріншот 3/3 (для Part 2)

    # 5. Запис даних у gold/avg_stats
    final_agg_df.write.mode("overwrite").parquet(GOLD_PATH)
    print(f"Дані успішно збережено в Gold Zone: {GOLD_PATH}")

    spark.stop()


if __name__ == "__main__":
    run_silver_to_gold()
