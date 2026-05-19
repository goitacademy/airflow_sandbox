import requests
from pyspark.sql import SparkSession

# ─────────────────────────────────────────
# Stage 1. Landing → Bronze
# Download CSV from FTP and save as Parquet
# ─────────────────────────────────────────

spark = SparkSession.builder \
    .appName("Landing to Bronze") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

FTP_BASE = "https://ftp.goit.study/neoversity"
TABLES   = ["athlete_bio", "athlete_event_results"]

for table in TABLES:
    url       = f"{FTP_BASE}/{table}.csv"
    local_csv = f"/tmp/{table}.csv"

    # Download CSV from FTP server
    print(f"Downloading {url}...")
    response = requests.get(url)
    with open(local_csv, "wb") as f:
        f.write(response.content)
    print(f"Saved to {local_csv}")

    # Read CSV with Spark
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(local_csv)

    print(f"=== {table} (bronze) ===")
    df.show(5)
    print(f"Row count: {df.count()}")

    # Save as Parquet in bronze/{table}
    output_path = f"bronze/{table}"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Written to {output_path}\n")

spark.stop()
