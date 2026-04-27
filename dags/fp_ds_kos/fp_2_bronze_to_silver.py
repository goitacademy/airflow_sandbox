import os
import sys


os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from text_cleaner import clean_text

spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

clean_text_udf = udf(clean_text, StringType())

tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    df = spark.read.parquet(f"bronze/{table}")

    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, clean_text_udf(df[col_name]))

    df = df.dropDuplicates()

    df.show(5, truncate=False)

    output_path = f"silver/{table}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Saved in {output_path}")
