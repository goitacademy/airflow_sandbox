import os
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import(
    col, 
    expr, 
    from_unixtime, 
    regexp_replace, 
    trim, 
    unix_timestamp, 
    avg
)


# Створюємо сесію Spark з іменем "SilverToGold"
spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

# Зчитуємо таблиці зі silver layer
event_results_df = spark.read.parquet(f"/tmp/silver/athlete_event_results")
bio_df_raw = spark.read.parquet(f"/tmp/silver/athlete_bio")

# Очищення даних bio_df: видаляємо NULL, порожні значення, конвертуємо в числа
bio_df = (
    bio_df_raw
    .dropna(subset=["height", "weight"])
    .filter((trim(col("height")) != "") & (trim(col("weight")) != ""))
    .withColumn("height", regexp_replace(col("height"), ",", "."))
    .withColumn("weight", regexp_replace(col("weight"), ",", "."))
    .withColumn("height", expr("try_cast(height as double)"))
    .withColumn("weight", expr("try_cast(weight as double)"))
    .filter(col("height").isNotNull() & col("weight").isNotNull())
    .filter((col("height") > 0) & (col("weight") > 0))
)

joined_df_raw = event_results_df.join(bio_df, on="athlete_id", how="inner")

joined_df = joined_df_raw.select(
    event_results_df["athlete_id"].alias("athlete_id"),
    event_results_df["sport"].alias("sport"),
    event_results_df["medal"].alias("medal"),
    bio_df["sex"].alias("sex"),
    bio_df["country_noc"].alias("country_noc"),
    bio_df["height"].alias("height"),
    bio_df["weight"].alias("weight"),
)

agg_df = (
    joined_df.groupBy("sport", "medal", "sex", "country_noc")
    .agg(
        avg(col("height")).alias("avg_height"),
        avg(col("weight")).alias("avg_weight"),
    )
    .withColumn("timestamp", from_unixtime(unix_timestamp()).cast("timestamp"))
)

agg_df.show()

output_path = f"/tmp/gold/avg_stats"
os.makedirs(output_path, exist_ok=True)
agg_df.write.mode("overwrite").parquet(output_path)

spark.stop()