import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import sys

# Configuration
BRONZE_LAYER = "bronze"
SILVER_LAYER = "silver"
TABLES = ["athlete_bio", "athlete_event_results"]

def clean_text(text):
    if text is None:
        return None
    return re.sub(r'[^a-zA-Z0-9,.\"\']', '', str(text))

def clean_dataframe(df):
    print(f"\nCleaning text columns...")

    # Creating UDF for cleaning text
    clean_text_udf = udf(clean_text, StringType())

    # Find all text columns
    string_columns = [
        field.name for field in df.schema.fields
        if field.dataType.simpleString() == 'string'
    ]
    print(f"Found {len(string_columns)} text columns: {string_columns}")

    # Apply cleaning to each text column
    cleaned_df = df
    for col_name in string_columns:
        cleaned_df = cleaned_df.withColumn(
            col_name,
            clean_text_udf(cleaned_df[col_name])
        )
    print(f"Text columns are cleaned")
    return cleaned_df

def deduplicate_dataframe(df):
    print(f"\nDeduplicate lines...")

    initial_count = df.count()
    deduplicated_df = df.dropDuplicates()
    final_count = deduplicated_df.count()

    duplicates_removed = initial_count - final_count

    print(f"Initial quantity: {initial_count:,}")
    print(f"After deduplication: {final_count:,}")
    print(f"Removed duplicates: {duplicates_removed:,}")

    if duplicates_removed > 0:
        print(f"Deduplication completed ({duplicates_removed:,} duplicates removed)")
    else:
        print(f"Duplicates not found")
    return deduplicated_df

def process_table(spark, table_name):
    print(f"\n{'=' * 80}")
    print(f"TABLE PROCESSING: {table_name}")
    print(f"{'=' * 80}")

    try:
        # Reading from Bronze Layer
        bronze_path = f'{BRONZE_LAYER}/{table_name}'
        print(f"\nReading from Bronze: {bronze_path}")

        df = spark.read.parquet(bronze_path)
        print(f" Data loaded from Bronze")
        print(f" Number of rows: {df.count():,}")
        print(f" Number of columns: {len(df.columns)}")

        print(f"\nData schema:")
        df.printSchema()

        print(f"\nFirst 5 lines (before processing):")
        df.show(5, truncate=False)

        # Step 1: Cleaning text
        cleaned_df = clean_dataframe(df)

        # Step 2: Deduplication
        final_df = deduplicate_dataframe(cleaned_df)

        # Showing result
        print(f"\nFirst 5 lines (after processing):")
        final_df.show(5, truncate=False)

        # Saving into Silver Layer
        silver_path = f"{SILVER_LAYER}/{table_name}"
        print(f"\nSaving in to Silver: {silver_path}")

        final_df.write.mode("overwrite").parquet(silver_path)
        print(f" Data saved into Silver")

        # Verifying
        verify_df = spark.read.parquet(silver_path)
        verify_count = verify_df.count()

        print(f"Verifying: {verify_count:,} lines in the Silver layer")

        print(f"\nTable {table_name} successfully processed!")
        print(f" Bronze: {bronze_path}")
        print(f" Silver: {silver_path}")

        return silver_path
    except Exception as e:
        print(f"Error processing table {table_name}: {e}")
        import traceback
        traceback.print_exc()
        raise

def main():
    print("\n" + "=" * 80) 
    print("BRONZE -> SILVER ETL PROCESS") 
    print("Building an End-to-End Batch Data Lake - Part 2") 
    print("=" * 80)

    # Creating Spark Session
    print("\nCreating Spark Session...")
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session is created")

    try:
        processed_tables = []

        # Processing each table
        for table_name in TABLES:
            silver_path = process_table(spark, table_name)
            processed_tables.append((table_name, silver_path))

        # Conclusion
        print(f"\n{'=' * 80}")
        print("SUMMARY BRONZE → SILVER")
        print(f"{'=' * 80}")
        print(f"Tables processed: {len(processed_tables)}")
        print(f"Bronze layer: {BRONZE_LAYER}/")
        print(f"Silver layer: {SILVER_LAYER}/")

        for table_name, path in processed_tables:
            print(f" - {path}")

        print(f"\nETL process completed successfully!")

    except Exception as e:
        print(f"\n✗ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()
        print("\nSpark session closed")

if __name__ == "__main__":
    main()

