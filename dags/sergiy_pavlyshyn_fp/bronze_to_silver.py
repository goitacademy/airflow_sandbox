import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


def clean_text(text):
    if text is None:
        return None
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', "", str(text))


clean_text_udf = udf(clean_text, StringType())


def main():
    spark = (
        SparkSession.builder.appName("Bronze_to_Silver")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        print(f"\nОбробка таблиці {table} (Bronze -> Silver)...")

        input_path = f"bronze/{table}"
        df = spark.read.parquet(input_path)

        for column in df.columns:
            if df.schema[column].dataType == StringType():
                print(f"Очищення колонки: {column}")
                df = df.withColumn(column, clean_text_udf(col(column)))

        df = df.dropDuplicates()

        output_path = f"silver/{table}"
        df.write.mode("overwrite").parquet(output_path)

        print(f"Таблиця {table} успішно очищена та збережена у {output_path}")

        print(f"Дані {table} після очищення (перші 5 рядків):")
        df.show(5)

    spark.stop()
    print("\nПроцес Bronze to Silver завершено успішно!")


if __name__ == "__main__":
    main()
