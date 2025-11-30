import requests
from pyspark.sql import SparkSession
import os
import sys

# Configuration
FTP_BASE_URL = "https://ftp.goit.study/neoversity/"
LANDING_ZONE = "landing"
BRONZE_LAYER = "bronze"

# Tables for downloading
TABLES = ["athlete_bio", "athlete_event_results"]

def download_data(table_name, landing_path):
    url = FTP_BASE_URL + table_name + ".csv"
    local_file = f"{landing_path}/{table_name}.csv"

    print(f"=" * 80)
    print(f"Downloading from FTP: {url}")
    print(f"Saving in the: {local_file}")
    print(f"=" * 80)

    try:
        response = requests.get(url)

        if response.status_code == 200:
            # creating directory if not exists
            os.makedirs(landing_path, exist_ok=True)

            # Saving files
            with open(local_file, 'wb') as file:
                file.write(response.content)

                file_size = os.path.getsize(local_file)
                print(f"File successfully uploaded")
                print(f"Size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
                return local_file
        else:
            print(f"Upload error. Status code: {response.status_code}")
            sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

def csv_to_parquet(spark, csv_path, parquet_path, table_name):
    print(f"\n" + "=" * 80)
    print(f"Conversion CSV -> Parquet: {table_name}")
    print(f"=" * 80)

    try:
        # Reading CSV with header and schema inference
        df = spark.read.csv(
            csv_path,
            header=True,
            inferSchema=True
        )

        row_count = df.count()
        print(f"CSV read successfully")
        print(f" Number of rows: {row_count:,}")
        print(f"Number of columns: {len(df.columns)}")
        print(f"Columns: {df.columns}")

        # Schema showing
        print(f"\nData schema:")
        df.printSchema()

        # Showing first the 5 lines
        print(f"\nFirst 5 lines:")
        df.show(5, truncate=False)

        # Creating a directory for Parquet
        output_path = f"{parquet_path}/{table_name}"

        # Saving in the Parquet format
        df.write.mode("overwrite").parquet(output_path)
        print(f"\nParquet successfully saved")
        print(f" Path: {output_path}")

        # Checking the saved data
        verify_df = spark.read.parquet(output_path)
        verify_count = verify_df.count()

        if verify_count == row_count:
            print(f"Verification successful: {verify_count:,} rows")
        else:
            print(f"WARNING: The number of lines does not match!")
            print(f"CSV: {row_count:,}, Parquet: {verify_count:,}")
        return output_path
    
    except Exception as e:
        print(f"Conversion error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def main():
    print("\n" + "=" * 80)
    print("LANDING -> BRONZE ETL PROCESS")
    print("Building and End-to-End Batch Data Lake - Part 2")
    print("=" * 80)

    # Creating Spark session
    print("\nCreating Spark session...")
    spark = SparkSession.builder \
        .appName("LandingToBronze") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session is created")

    try:
        # Processing each table
        for table_name in TABLES:
            print(f"\n{'=' * 80}")
            print(f"PROCESS TABLE: {table_name}")
            print(f"{'=' * 80}")

            # Step 1: Downloading CSV from FTP
            csv_file = download_data(table_name, LANDING_ZONE)

            # Step 2: Conversion CSV -> Parquet
            parquet_path = csv_to_parquet(
                spark,
                csv_file,
                BRONZE_LAYER,
                table_name
            )

            print(f"Table {table_name} processed successfully!")
            print(f"Landing: {csv_file}")
            print(f"Bronze: {parquet_path}")

        # Conclusion
        print(f"\n{'=' * 80}")
        print("CONCLUSION LANDING -> BRONZE")
        print(f"{'=' * 80}")
        print(f"Tables processed: {len(TABLES)}")
        print(f"Landing zone: {LANDING_ZONE}")
        print(f"Bronze layer: {BRONZE_LAYER}")

        for table in TABLES:
            print(f" - {BRONZE_LAYER}/{table}/")
        print(f"\n ETL process was completed successfully!")
    
    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()
        print("\nSpark session is closed")

if __name__ == "__main__":
    main()