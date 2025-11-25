
# silver_to_gold.py має:
# зчитувати дві таблиці: silver/athlete_bio та silver/athlete_event_results,
# робити join за колонкою athlete_id,
# для кожної комбінації цих 4 стовпчиків — sport, medal, sex, country_noc — знаходити середні значення weight і height,
# додати колонку timestamp з часовою міткою виконання програми,
# записувати дані в gold/avg_stats.

# Колонки, з якими буде зроблено математичні перетворення (weight і height), повинні мати тип Integer,
# Float чи Double. Колонки типу String мають бути приведені до чисельного типу.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp, expr

def remove_duplicate_columns(df1, df2):
    final_cols = []
    seen = set()

    for c in df1.columns:
        if c in df2.columns:
            if c != 'athlete_id':
                df2 = df2.drop(c)

    return df2


spark = (
    SparkSession.builder
    .appName("SilverToGoldAvgStats") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1. Читаємо таблиці з silver
bio_path = "silver/athlete_bio"
results_path = "silver/athlete_event_results"

athlete_bio = spark.read.parquet(bio_path)
athlete_results = spark.read.parquet(results_path)

# 2. Join за athlete_id
athlete_results = remove_duplicate_columns(athlete_bio, athlete_results)
joined = (
    athlete_results
    .join(athlete_bio, on="athlete_id", how="inner")
)

joined.printSchema()
joined.show(5)

# 3. Приводимо weight і height до числового типу (double)
#    Якщо вони вже numeric, cast не зашкодить.
joined_numeric = (
    joined
    .withColumn("weight", expr("try_cast(weight as double)"))
    .withColumn("height", expr("try_cast(height as double)"))
)

# 4. Агрегація за sport, medal, sex, country_noc

agg_df = (joined_numeric
            .select(col("medal"), col("sex"), col("country_noc"), col("sport"), col("height"), col("weight")) \
            .filter(col("sex").isNotNull() & col("country_noc").isNotNull() & col("sport").isNotNull()) \
            .groupBy("sport", "medal", "sex", "country_noc") \
            .agg(avg(col("height")).alias("avg_height"), avg(col("weight")).alias("avg_weight")) \
            .withColumn("timestamp", current_timestamp())
)

# 5. Запис до gold/avg_stats
output_path = "gold/avg_stats"
(
    agg_df.write
    .mode("overwrite")   # щоб можна було запускати повторно
    .parquet(output_path)
)

spark.stop()
print("Готово: дані збережено в gold/avg_stats")