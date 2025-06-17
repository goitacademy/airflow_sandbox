from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    # Зчитуємо дві таблиці із silver
    df_bio = spark.read.parquet("data/silver/athlete_bio")
    df_results = spark.read.parquet("data/silver/athlete_event_results")

    # Приводимо числові колонки до правильних типів
    df_bio = df_bio.withColumn("weight", col("weight").cast("float")) \
                   .withColumn("height", col("height").cast("float"))

    # Припустимо, що об’єднуємо по athlete_id
    df_gold = df_results.join(df_bio, "athlete_id", "inner")

    # Можна додати додаткові перетворення/фільтри...

    df_gold.write.mode("overwrite").parquet("data/gold/athlete_final")

    spark.stop()

if __name__ == "__main__":
    main()
