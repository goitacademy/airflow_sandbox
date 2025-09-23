from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
import os

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\' ]', '', str(text)) if text else None

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    clean_text_udf = udf(clean_text, StringType())

    for table in ["athlete_bio", "athlete_event_results"]:
        df = spark.read.parquet(f"bronze/{table}")

        # Очищення текстових колонок
        for col_name, dtype in df.dtypes:
            if dtype == "string":
                df = df.withColumn(col_name, clean_text_udf(df[col_name]))

        # Дедублікація
        df = df.dropDuplicates()

        print(f"=== {table} cleaned sample ===")
        df.show(5, truncate=False)

        # Збереження
        output_path = f"silver/{table}"
        df.write.mode("overwrite").parquet(output_path)
        print(f"✅ Збережено у {output_path}")
