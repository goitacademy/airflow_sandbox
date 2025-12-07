import re
import os
import sys
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col, when, trim, lower, regexp_replace
from pyspark.sql.types import StringType

if __name__ == "__main__":
    # Init Spark Session
    spark = SparkSession.builder \
            .appName(f"SilverToGoldPipeline") \
            .getOrCreate()

    # Add paths to files
    input_path_bio = os.path.join("silver", "athlete_bio.parquet")
    input_path_res = os.path.join("silver", "athlete_event_results.parquet")
    output_path = os.path.join("gold", "avg_stats.parquet")

    try:
        # Read the data from files
        df_bio_silver = spark.read.parquet(input_path_bio).select("athlete_id", "sex", "weight", "height")
        df_res_silver = spark.read.parquet(input_path_res).select("athlete_id", "country_noc", "sport", "medal")

        # Join the datasets about evert results and bio data by column athlete_id
        print("Joining datasets by column athlete_id")
        joined_df = df_res_silver.join(df_bio_silver, on="athlete_id", how="inner")

        # Changing comma by dot for height and weight and transform to Double type
        df_corrected = joined_df.filter(
        col("height").isNotNull() & col("weight").isNotNull()
        ).withColumn("height", regexp_replace(col("height"), ',', '.')
        ).withColumn("height",col("height").cast("double")
        ).withColumn("weight", regexp_replace(col("weight"), ',', '.')
        ).withColumn("weight",col("weight").cast("double"))

        # Find average value of height and weight for combination (sport, medal, sex, country_noc) and add column timestamp
        print("Find average value of height and weight for combination (sport, medal, sex, country_noc)")
        df_agg = (df_corrected
            .select("sport", "medal", "sex", "country_noc", "height", "weight")
            .groupBy("sport", "medal", "sex", "country_noc").agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight"),
            ).withColumn(
                "timestamp",
                current_timestamp())
        )
        df_agg.show(20)

        # Write table in folder gold in format Parquet
        print("Saving golden data")
        df_agg.write.mode("overwrite").parquet(output_path)

        print(f"Process Silver -> Gold finished successfully.")

    except Exception as e:
        print(f"{e}")
        sys.exit(1)

    finally:
        # Stop Spark Session
        spark.stop()
        print("Spark Session stopped.")