from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
import os


spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

# read tables from silver layer
athlete_bio_df = spark.read.parquet("silver/athlete_bio")
athlete_event_results_df = spark.read.parquet("silver/athlete_event_results")

# rename duplicating column for joining tables
athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "b_country_noc")

# join tables on the "athlete_id" column
joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

# calculate average weight and height for each combination of sport, medal, sex, country_noc columns
agg_df = (joined_df.groupBy("sport", "medal", "sex", "country_noc")
          .agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight"),
                current_timestamp().alias("timestamp")
                )
          )

# create a directory to save the results in the gold layer
output_path = "gold/avg_stats"
os.makedirs(output_path, exist_ok=True)

# save the processed data in parquet format
agg_df.write.mode("overwrite").parquet(output_path)

print(f"Successfully processed data and saved to {output_path}")

# show final dataframe for the job
df = spark.read.parquet(output_path)
df.show(truncate=False)

spark.stop()