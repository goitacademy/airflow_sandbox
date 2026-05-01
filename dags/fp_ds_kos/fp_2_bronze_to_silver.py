import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, expr

spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

BRONZE_DIR = os.path.join(BASE_DIR, "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "silver")

tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    input_path = os.path.join(BRONZE_DIR, table)
    output_path = os.path.join(SILVER_DIR, table)

    print(f"Reading from: {input_path}")

    df = spark.read.parquet(input_path)

    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(
                col_name,
                trim(
                    regexp_replace(
                        col(col_name),
                        r'[^a-zA-Z0-9,.\\"\' ]',
                        ''
                    )
                )
            )

    if table == "athlete_bio":
        df = df \
            .withColumn("height", expr("try_cast(height as double)")) \
            .withColumn("weight", expr("try_cast(weight as double)")) \
            .filter(col("height").isNotNull()) \
            .filter(col("weight").isNotNull())

    print(f"Writing to: {output_path}")

    df.write.mode("overwrite").parquet(output_path)

    print(f"Saved in {output_path}")

spark.stop()

print("Bronze to Silver Successfully Done")