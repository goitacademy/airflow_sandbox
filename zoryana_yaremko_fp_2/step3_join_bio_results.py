from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, regexp_extract

spark = (SparkSession.builder
         .appName("JoinAndAggregate")
         .getOrCreate())

# --- читаємо athlete_bio ---
df_bio = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv("data/landing/athlete_bio.csv"))

# очищення height/weight: залишаємо тільки цифри
df_bio_clean = (df_bio
    .withColumn("height_num", regexp_extract(col("height").cast("string"), r"(\d+)", 1).cast("double"))
    .withColumn("weight_num", regexp_extract(col("weight").cast("string"), r"(\d+)", 1).cast("double"))
    .filter(col("height_num").isNotNull() & col("weight_num").isNotNull())
    .select("athlete_id", "sex", "height_num", "weight_num",
            col("country_noc").alias("country_noc_bio"))
)

# --- читаємо athlete_event_results ---
df_results = (spark.read
              .option("header", True)
              .option("inferSchema", True)
              .csv("data/landing/athlete_event_results.csv")
              .withColumnRenamed("country_noc", "country_noc_res"))

# --- join ---
df_joined = df_results.join(df_bio_clean, on="athlete_id", how="inner")

# --- агрегація ---
df_agg = (df_joined.groupBy("sport", "medal", "sex", "country_noc_res")
          .agg(
              avg("height_num").alias("avg_height"),
              avg("weight_num").alias("avg_weight")
          )
          .withColumn("calc_timestamp", current_timestamp())
)

print(">>> Схема результату:")
df_agg.printSchema()

print(">>> Перші 10 рядків результату:")
df_agg.show(10, truncate=False)

spark.stop()



