import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName('Landing To Silver').getOrCreate()

table_names = ['athlete_event_results', 'athlete_bio']

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())

def parse_table(table_name):
    df = spark.read.parquet(f'bronze/{table_name}')

    for column_name, column_type in df.dtypes:
        if column_type == 'string':
            df = df.withColumn(
                column_name,
                clean_text_udf(col(column_name))
            )

    return df

def main():
    for table_name in table_names:
        full_path = f'silver/{table_name}'

        df = parse_table(table_name)

        (df.write
         .mode('overwrite')
         .parquet(full_path)
         )


if __name__ == '__main__':
    main()
