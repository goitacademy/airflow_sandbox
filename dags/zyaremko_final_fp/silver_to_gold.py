from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp

def main():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    bio_path = "dags/zyaremko_final_fp/datalake/silver/athlete_bio"
    results_path = "dags/zyaremko_final_fp/datalake/silver/athlete_event_results"

    bio = spark.read.parquet(bio_path)
    results = spark.read.parquet(results_path)

    # джойн за athlete_id
    df = results.join(bio, on="athlete_id", how="inner")

    # приведення числових колонок
    df = df.withColumn("weight", col("weight").cast("double"))
    df = df.withColumn("height", col("height").cast("double"))

    # агрегація
    agg = (
        df.groupBy("sport", "medal", "sex", "country_noc")
          .agg(
              avg("weight").alias("avg_weight"),
              avg("height").alias("avg_height")
          )
          .withColumn("ts", current_timestamp())
    )

    agg.show(10, truncate=False)

    output_path = "dags/zyaremko_final_fp/datalake/gold/avg_stats"
    agg.write.mode("overwrite").parquet(output_path)
    print(f"Written gold table: {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()



