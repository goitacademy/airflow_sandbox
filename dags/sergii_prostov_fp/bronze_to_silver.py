"""bronze_to_silver.py — Stage 2 of the multi-hop datalake.

Reads bronze/<table>/, applies a regex-based text-cleanup UDF to every string
column, drops duplicate rows, and writes silver/<table>/.

Usage (also how Airflow invokes it):
    spark-submit bronze_to_silver.py <table>
"""

from __future__ import annotations

import re
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def clean_text(text: object) -> str:
    return re.sub(r"[^a-zA-Z0-9,.\"']", "", str(text))


clean_text_udf = udf(clean_text, StringType())


def main(table: str) -> None:
    spark = (
        SparkSession.builder
        .appName(f"bronze_to_silver[{table}]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    bronze_path = f"bronze/{table}"
    silver_path = f"silver/{table}"

    df = spark.read.parquet(bronze_path)
    print(f"read {df.count()} rows from {bronze_path}")

    string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
    print(f"cleaning string columns: {string_cols}")
    for col_name in string_cols:
        df = df.withColumn(col_name, clean_text_udf(df[col_name]))

    deduped = df.dropDuplicates()
    print(f"writing {deduped.count()} rows -> {silver_path}")
    deduped.write.mode("overwrite").parquet(silver_path)

    deduped.show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: bronze_to_silver.py <table>")
    main(sys.argv[1])
