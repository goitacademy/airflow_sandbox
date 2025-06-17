import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())

def main():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    # Зчитуємо дані з bronze
    df = spark.read.parquet("data/bronze/athlete_bio")

    # Очищення текстових колонок
    text_cols = [field.name for field in df.schema.fields if field.dataType == StringType()]

    for col_name in text_cols:
        df = df.withColumn(col_name, clean_text_udf(df[col_name]))

    # Видалення дублікатів
    df = df.dropDuplicates()

    # Запис у silver
    df.write.mode("overwrite").parquet("data/silver/athlete_bio")

    spark.stop()

if __name__ == "__main__":
    main()
