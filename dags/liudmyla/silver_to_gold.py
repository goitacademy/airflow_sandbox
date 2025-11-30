from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp
import sys

# Config
SILVER_LAYER = "silver"
GOLD_LAYER = "gold"

TABLE_BIO = "athlete_bio"
TABLE_EVENTS = "athlete_event_results"
OUTPUT_TABLE = "avg_stats"

def load_silver_tables(spark):
    print(f"\n{'=' * 80}")
    print("LOADING DATA FROM SILVER LAYER")
    print(f"{'=' * 80}")

    # Downloading athlete_bio
    bio_path = f"{SILVER_LAYER}/{TABLE_BIO}"
    print(f"\nReading: {bio_path}")

    bio_df = spark.read.parquet(bio_path)
    bio_count = bio_df.count()

    print(f" athlete_bio loaded")
    print(f" Row count: {bio_count:,}")
    print(f" Columns: {bio_df.columns}")

    print(f"\nSchema athlete_bio:")
    bio_df.printSchema()

    print(f"\nFirst 3 rows athlete_bio:")
    bio_df.show(3, truncate=False)

    # Downloading athlete_event_results
    events_path = f"{SILVER_LAYER}/{TABLE_EVENTS}"
    print(f"\nReading: {events_path}")

    events_df = spark.read.parquet(events_path)
    events_count = events_df.count()

    print(f" athlete_event_results loaded")
    print(f" Row count: {events_count:,}")
    print(f" Columns: {events_df.columns}")

    print(f"\nSchema athlete_event_results:")
    events_df.printSchema()

    print(f"\nFirst 3 rows athlete_event_results:")
    events_df.show(3, truncate=False)

    return bio_df, events_df

def join_and_prepare_data(bio_df, events_df):
    print(f"\n{'=' * 80}")
    print("JOIN TABLES")
    print(f"{'=' * 80}")

    events_df= events_df.select(
        "athlete_id", "sport", "medal", "country_noc"
    )
    bio_df = bio_df.select(
        "athlete_id", "sex", "height", "weight"  # here we can leave out country_noc because events_df has one
    )

    # JOIN on athlete_id
    print(f"\nExecuting JOIN on athlete_id...")

    joined_df = events_df.join(
        bio_df,
        on="athlete_id",
        how="inner"
    )

    joined_count = joined_df.count()
    print(f"✓ JOIN done")
    print(f" Row count: {joined_count:,}")

    # Check for required columns
    required_cols = ["sport", "medal", "sex", "country_noc", "weight", "height"]
    missing_cols = [c for c in required_cols if c not in joined_df.columns]

    if missing_cols:
        raise ValueError(f"Required columns are missing: {missing_cols}")

    print(f"All required columns are present: {required_cols}")

    # Convert weight and height to numeric type
    print(f"\nConverting weight and height to numeric type...")

    prepared_df = joined_df \
        .withColumn("weight", col("weight").cast("double")) \
        .withColumn("height", col("height").cast("double"))
    
    # Filter NULL values ​​in weight and height
    prepared_df = prepared_df.filter(
        col("weight").isNotNull() &
        col("height").isNotNull()
    )

    filtered_count = prepared_df.count()
    removed = joined_count - filtered_count

    print(f" Conversion done")
    print(f" Rows after filtering NULL: {filtered_count:,}")
    print(f" Rows removed from NULL: {removed:,}")

    print(f"\nFirst 5 rows after JOIN:")
    prepared_df.select(
        "athlete_id", "sport", "medal", "sex", "country_noc", "weight", "height"
    ).show(5, truncate=False)

    return prepared_df

def calculate_aggregates(df):
    print(f"\n{'=' * 80}")
    print("AGGREGATE CALCULATION")
    print(f"{'=' * 80}")

    print(f"\nGrouping by: sport, medal, sex, country_noc")
    print(f"Aggregation: AVG(weight), AVG(height)")

    # Grouping and aggregation
    agg_df = df.groupBy(
        "sport",
        "medal",
        "sex",
        "country_noc"
    ).agg(
        avg("weight").alias("avg_weight"),
        avg("height").alias("avg_height")
    )

    # adding timestamp
    agg_df = agg_df.withColumn("timestamp", current_timestamp())

    agg_count = agg_df.count()
    print(f" Aggregation done")
    print(f" Number of unique combinations: {agg_count:,}")

    print(f"\nResult schema:")
    agg_df.printSchema()

    print(f"\nFirst 10 rows of result:")
    agg_df.orderBy(col("avg_height").desc()).show(10, truncate=False)

    # Statistics
    print(f"\nStatistics by sport:")
    agg_df.groupBy("sport").count().orderBy(col("count").desc()).show(10)

    print(f"\nStatistics by medal:")
    agg_df.groupBy("medal").count().show()

    return agg_df

def save_to_gold(df, output_path):
    print(f"\n{'=' * 80}")
    print("SAVED TO GOLD LAYER")
    print(f"{'=' * 80}")

    print(f"\nSaved to: {output_path}")

    df.write.mode("overwrite").parquet(output_path)

    print(f"Data saved to Gold layer")

    # Check
    verify_df = spark.read.parquet(output_path)
    verify_count = verify_df.count()

    print(f"Check: {verify_count:,} lines in the Gold Layer")

    return output_path

def main():
    print("\n" + "=" * 80)
    print("SILVER → GOLD ETL PROCESS")
    print("Building an End-to-End Batch Data Lake - Part 2")
    print("=" * 80)

    # Create a Spark session
    print("\nCreating a Spark session...")
    global spark
    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    


    spark.sparkContext.setLogLevel("WARN")
    print("✓ Spark session created")

    try:
        # Step 1: Load data from Silver
        bio_df, events_df = load_silver_tables(spark)

        # Step 2: JOIN and prepare data
        prepared_df = join_and_prepare_data(bio_df, events_df)

        # Step 3: Calculate aggregates
        agg_df = calculate_aggregates(prepared_df)

        # Step 4: Save to Gold
        output_path = f"{GOLD_LAYER}/{OUTPUT_TABLE}"
        save_to_gold(agg_df, output_path)

        # Summary
        print(f"\n{'=' * 80}")
        print("SUMMARY SILVER → GOLD")
        print(f"{'=' * 80}")
        print(f" Input tables:")
        print(f" - {SILVER_LAYER}/{TABLE_BIO}")
        print(f" - {SILVER_LAYER}/{TABLE_EVENTS}")
        print(f" Output table:")
        print(f" - {output_path}")
        print(f" Aggregate combinations: {agg_df.count():,}")

        print(f"\nETL process completed successfully!")

    except Exception as e:
        print(f"\n✗ FATE: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()
        print("\n✓ Spark session closed")

if __name__ == "__main__":
    main()
