import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, trim, regexp_replace
from pyspark.sql.types import StringType

# ─────────────────────────────────────────
# Stage 2. Bronze → Silver
# Clean text columns + deduplicate
# ─────────────────────────────────────────

spark = SparkSession.builder \
    .appName("Bronze to Silver") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

TABLES = ["athlete_bio", "athlete_event_results"]

def clean_text(value):
    """Remove special characters, extra whitespace from text."""
    if value is None:
        return None
    # Remove non-printable and special characters
    value = re.sub(r'[^\x20-\x7E]', '', value)
    # Collapse multiple spaces
    value = re.sub(r'\s+', ' ', value).strip()
    return value

clean_text_udf = udf(clean_text, StringType())

for table in TABLES:
    input_path  = f"bronze/{table}"
    output_path = f"silver/{table}"

    # Read from bronze
    df = spark.read.parquet(input_path)

    # Apply text cleaning to all string columns
    string_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]
    for c in string_cols:
        df = df.withColumn(c, clean_text_udf(col(c)))

    # Deduplicate
    df = df.dropDuplicates()

    print(f"=== {table} (silver) ===")
    df.show(5)
    print(f"Row count: {df.count()}")

    # Write to silver
    df.write.mode("overwrite").parquet(output_path)
    print(f"Written to {output_path}\n")

spark.stop()
