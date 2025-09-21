from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def main():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    # читаємо parquet з silver
    input_path = "dags/zyaremko_final_fp/datalake/silver/orders"
    df = spark.read.parquet(input_path)

    # приклад агрегації: кількість замовлень по кожному користувачу
    df_gold = df.groupBy("user_id").agg(count("*").alias("total_orders"))

    df_gold.show(5)

    # пишемо у gold
    output_path = "dags/zyaremko_final_fp/datalake/gold/orders_by_user"
    df_gold.write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()



