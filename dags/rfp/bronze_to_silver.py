import re
import os

DAG_DIR = os.path.dirname(os.path.abspath(__file__))

table_names = ['athlete_event_results', 'athlete_bio']


def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\"\']', '', str(text))


def parse_and_save_table(spark, table_name):
    from pyspark.sql.functions import udf, col
    from pyspark.sql.types import StringType

    clean_text_udf = udf(clean_text, StringType())

    bronze_path = os.path.join(DAG_DIR, 'bronze', table_name)
    silver_path = os.path.join(DAG_DIR, 'silver', table_name)

    df = spark.read.parquet(bronze_path)

    for column_name, column_type in df.dtypes:
        if column_type == 'string':
            df = df.withColumn(
                column_name,
                clean_text_udf(col(column_name))
            )

    (df.write
     .mode('overwrite')
     .parquet(silver_path))

    df.show()

    print(f"Table '{table_name}' saved to {silver_path}")


def main():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName('Bronze To Silver').getOrCreate()

    try:
        for table_name in table_names:
            parse_and_save_table(spark, table_name)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
