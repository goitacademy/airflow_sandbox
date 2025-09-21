from pyspark.sql import SparkSession

# ====== SparkSession ======
spark = (SparkSession.builder
         .appName("ReadAthleteEventResultsCSV")
         .getOrCreate())

# ====== Читаємо CSV з результатами ======
df_results = (spark.read
              .option("header", True)
              .option("inferSchema", True)
              .csv("data/landing/athlete_event_results.csv"))

print(">>> Схема таблиці athlete_event_results:")
df_results.printSchema()

print(">>> Перші 10 рядків:")
df_results.show(10, truncate=False)

spark.stop()
