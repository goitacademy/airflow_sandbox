from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, expr


def main():
    spark = (
        SparkSession.builder.appName("Silver_to_Gold").master("local[*]").getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Зчитуємо таблиці зі Срібного рівня...")

    bio_df = spark.read.parquet("silver/athlete_bio")
    results_df = spark.read.parquet("silver/athlete_event_results")

    print("Виконуємо об'єднання (Join) та підготовку даних...")

    if "country_noc" in bio_df.columns and "country_noc" in results_df.columns:
        bio_clean_df = bio_df.drop("country_noc")
    else:
        bio_clean_df = bio_df

    joined_df = results_df.join(bio_clean_df, on="athlete_id", how="inner")

    joined_df = joined_df.withColumn(
        "height", expr("try_cast(height as float)")
    ).withColumn("weight", expr("try_cast(weight as float)"))

    print("Групуємо дані та рахуємо середні значення...")

    gold_df = (
        joined_df.groupBy("sport", "medal", "sex", "country_noc")
        .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight"))
        .withColumn("timestamp", current_timestamp())
    )

    output_path = "gold/avg_stats"
    print(f"Записуємо фінальний результат у {output_path}...")

    gold_df.write.mode("overwrite").parquet(output_path)

    print("Золота таблиця готова (перші 5 рядків для звіту):")
    gold_df.show(5, truncate=False)

    spark.stop()
    print(
        "Процес Silver to Gold завершено успішно! Медальйонна архітектура побудована."
    )


if __name__ == "__main__":
    main()
