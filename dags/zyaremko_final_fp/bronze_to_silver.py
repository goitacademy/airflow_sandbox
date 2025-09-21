import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())

def process_table(spark, table):
    input_path = f"dags/zyaremko_final_fp/datalake/bronze/{table}"
    output_path = f"dags/zyaremko_final_fp/datalake/silver/{table}"

    df = spark.read.parquet(input_path)

    # чистка текстових колонок
    for column, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(column, clean_text_udf(col(column)))

    # дедублікація
    df = df.dropDuplicates()

    df.show(5, truncate=False)
    df.write.mode("overwrite").parquet(output_path)
    print(f"Written silver table: {output_path}")

def main():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    for table in ["athlete_bio", "athlete_event_results"]:
        process_table(spark, table)
    spark.stop()

if __name__ == "__main__":
    main()


