# NEW VERSION THAT MATCHES YOUR FOLDER STRUCTURE

from pyspark.sql import SparkSession
import os

def process_table(table):
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    # Your CSVs are here:
    local_path = f"/usr/local/spark/app/data/{table}.csv"

    # Parquet output
    output_path = f"/tmp/bronze/{table}"
    os.makedirs(output_path, exist_ok=True)

    # Read CSV
    df = spark.read.csv(local_path, header=True, inferSchema=True)

    # Save parquet
    df.write.mode("overwrite").parquet(output_path)

    print(f"Saved to {output_path}")
    df.show(truncate=False)

    spark.stop()


def main():
    tables = ["athlete_bio", "athlete_event_results"]
    for table in tables:
        process_table(table)

if __name__ == "__main__":
    main()
