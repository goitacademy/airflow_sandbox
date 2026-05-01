from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, expr

spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    df = spark.read.parquet(f"bronze/{table}")

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

    df.show(5, truncate=False)

    output_path = f"silver/{table}"
    df.write.mode("overwrite").parquet(output_path)

    print(f"Saved in {output_path}")
