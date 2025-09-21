from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    # читаємо CSV з landing
    input_path = "dags/zyaremko_final_fp/datalake/landing/orders.csv"
    df = spark.read.option("header", True).csv(input_path)

    df.show(5)

    # пишемо у bronze
    output_path = "dags/zyaremko_final_fp/datalake/bronze/orders"
    df.write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()


