from pyspark.sql import SparkSession

from downloader import download_data


spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    download_data(table)

    df = spark.read \
        .option("header", True) \
        .csv(f"landing/{table}.csv")

    print(f"\nTable: {table}")
    df.show(5, truncate=False)

    output_path = f"bronze/{table}"

    df.write \
        .mode("overwrite") \
        .parquet(output_path)

    print(f"Saved in {output_path}")