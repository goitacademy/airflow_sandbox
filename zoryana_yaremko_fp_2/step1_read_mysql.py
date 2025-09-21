from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ====== Створюємо SparkSession ======
spark = (SparkSession.builder
         .appName("ReadAthleteBioCSV")
         .getOrCreate())

# ====== Локальне читання CSV ======
df = (spark.read
      .option("header", True)
      .option("inferSchema", True)  # Spark сам визначить типи
      .csv("data/landing/athlete_bio.csv"))

print(">>> Схема таблиці athlete_bio (локально з CSV):")
df.printSchema()

print(">>> Перші 10 рядків:")
df.show(10, truncate=False)

# ====== Фільтрація height/weight ======
df_clean = df.filter(
    (col("height").cast("double").isNotNull()) &
    (col("weight").cast("double").isNotNull())
)

print(">>> Після фільтрації height/weight:")
df_clean.show(10, truncate=False)

spark.stop()

