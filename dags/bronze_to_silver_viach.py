from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import re
import os

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Bronze to Silver") \
        .getOrCreate()
    
    # Create UDF for text cleaning
    clean_text_udf = udf(clean_text, StringType())
    
    tables = ["athlete_bio", "athlete_event_results"]
    
    for table in tables:
        # Read from bronze
        bronze_path = f"bronze/{table}"
        df = spark.read.parquet(bronze_path)
        
        # Get text columns (string type)
        text_columns = [field.name for field in df.schema.fields 
                        if isinstance(field.dataType, StringType)]
        
        # Apply text cleaning to all text columns
        for column in text_columns:
            df = df.withColumn(column, clean_text_udf(col(column)))
        
        # Remove duplicates
        df = df.dropDuplicates()
        
        # Create silver directory if it doesn't exist
        silver_path = f"silver/{table}"
        os.makedirs(os.path.dirname(silver_path), exist_ok=True)
        
        # Save to silver
        df.write.mode("overwrite").parquet(silver_path)
        
        print(f"✅ Saved {table} to silver layer")
        df.show()

    spark.stop()

if __name__ == "__main__":
    main()