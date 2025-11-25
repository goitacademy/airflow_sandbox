from pyspark.sql import SparkSession
import os

def process_table(table):
    spark = (
        SparkSession.builder
        .appName("LandingToBronze")
        .getOrCreate()
    )

    # CSV-файли ти поклала в spark/app/data
    local_path = f"/usr/local/spark/app/data/{table}.csv"

    output_path = f"/tmp/bronze/{table}"
    os.makedirs(output_path, exist_ok=True)

    df = (
        spark.read
        .csv(local_path, header=True, inferSchema=True)
    )

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
