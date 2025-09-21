from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    # читаємо parquet з bronze
    input_path = "dags/zyaremko_final_fp/datalake/bronze/orders"
    df = spark.read.parquet(input_path)

    # приклад очистки: прибираємо рядки без order_id
    df_clean = df.filter(col("order_id").isNotNull())

    df_clean.show(5)

    # пишемо у silver
    output_path = "dags/zyaremko_final_fp/datalake/silver/orders"
    df_clean.write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()

