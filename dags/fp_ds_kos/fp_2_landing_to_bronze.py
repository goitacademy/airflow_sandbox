from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

urls = {
    "athlete_bio": "https://ftp.goit.study/neoversity/athlete_bio.csv",
    "athlete_event_results": "https://ftp.goit.study/neoversity/athlete_event_results.csv",
}

for table in ["athlete_bio", "athlete_event_results"]:
    input_url = urls[table]
    output_path = f"bronze/{table}"

    print(f"Reading from URL: {input_url}")

    df = spark.read \
        .option("header", True) \
        .csv(input_url)

    df.show(5, truncate=False)

    df.write.mode("overwrite").parquet(output_path)

    print(f"Saved in {output_path}")