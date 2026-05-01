import os

from pyspark.sql import SparkSession
from downloader import download_data

spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

landing_dir = os.path.join(BASE_DIR, "landing")
bronze_dir = os.path.join(BASE_DIR, "bronze")

tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    download_data(table, landing_dir)

    input_path = os.path.join(landing_dir, f"{table}.csv")
    output_path = os.path.join(bronze_dir, table)

    print(f"Reading from: {input_path}")

    df = spark.read \
        .option("header", True) \
        .csv(input_path)

    df.show(5, truncate=False)

    print(f"Writing to: {output_path}")

    df.write \
        .mode("overwrite") \
        .parquet(output_path)

    print(f"Saved in {output_path}")