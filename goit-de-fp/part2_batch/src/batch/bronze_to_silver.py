"""
Bronze to Silver - Clean and deduplicate data
Part 2, Step 2 of the Final Project
Updated for GoIT External Services with fefelov prefix
"""
import logging
import os
import sys
import re
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - fefelov_%(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def clean_text(text) -> str:
    """
    Clean text by removing non-alphanumeric characters except basic punctuation
    
    Args:
        text: Input text to clean
        
    Returns:
        Cleaned text string
    """
    if text is None:
        return ""
    return re.sub(r'[^a-zA-Z0-9,."\'\\]', '', str(text))


# Create UDF for text cleaning
clean_text_udf = udf(clean_text, StringType())


def clean_text_columns(df: DataFrame) -> DataFrame:
    """
    Apply text cleaning to all string columns in DataFrame
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with cleaned text columns
    """
    logger.info("Cleaning text columns...")
    
    # Get string columns
    string_columns = [field.name for field in df.schema.fields 
                     if field.dataType == StringType()]
    
    logger.info(f"Found {len(string_columns)} string columns to clean: {string_columns}")
    
    # Apply cleaning to each string column
    cleaned_df = df
    for col_name in string_columns:
        cleaned_df = cleaned_df.withColumn(col_name, clean_text_udf(col(col_name)))
    
    return cleaned_df


def deduplicate_data(df: DataFrame) -> DataFrame:
    """
    Remove duplicate rows from DataFrame
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with duplicates removed
    """
    logger.info("Removing duplicate rows...")
    
    initial_count = df.count()
    deduplicated_df = df.dropDuplicates()
    final_count = deduplicated_df.count()
    
    logger.info(f"Removed {initial_count - final_count} duplicate rows")
    logger.info(f"Records: {initial_count} -> {final_count}")
    
    return deduplicated_df


def process_bronze_to_silver(table_name: str, config: Config) -> None:
    """
    Process a single table from bronze to silver
    
    Args:
        table_name: Name of the table to process
        config: Configuration object with paths and settings
    """
    logger.info(f"Processing {table_name} from bronze to silver...")
    
    # Initialize Spark session with prefixed app name
    spark = SparkSession.builder \
        .appName(f"{config.student_prefix}_BronzeToSilver_{table_name}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Step 1: Read table from bronze layer
        bronze_path = f"{config.bronze_path}/{table_name}"
        logger.info(f"Reading from bronze layer: {bronze_path}")
        
        df = spark.read.parquet(bronze_path)
        initial_count = df.count()
        logger.info(f"Loaded {initial_count} records from bronze layer")
        
        # Step 2: Clean text columns
        cleaned_df = clean_text_columns(df)
        
        # Step 3: Remove duplicates
        final_df = deduplicate_data(cleaned_df)
        
        # Step 4: Save to silver layer
        silver_path = f"{config.silver_path}/{table_name}"
        logger.info(f"Saving to silver layer: {silver_path}")
        
        final_df.write \
            .mode("overwrite") \
            .parquet(silver_path)
        
        logger.info(f"Successfully saved {table_name} to silver layer")
        
        # Show sample data for verification
        logger.info("Sample cleaned data:")
        final_df.show(5, truncate=False)
        
    except Exception as e:
        logger.error(f"Error processing {table_name}: {str(e)}")
        raise
    finally:
        spark.stop()


def main() -> None:
    """
    Main function to process all tables from bronze to silver
    """
    logger.info("Starting Bronze to Silver processing with fefelov prefix...")
    
    # Load configuration
    config = Config()
    
    # List of tables to process
    tables = ["athlete_bio", "athlete_event_results"]
    
    # Create silver directory
    os.makedirs(config.silver_path, exist_ok=True)
    
    logger.info(f"Using student prefix: {config.student_prefix}")
    logger.info(f"Bronze path: {config.bronze_path}")
    logger.info(f"Silver path: {config.silver_path}")
    
    for table_name in tables:
        try:
            process_bronze_to_silver(table_name, config)
        except Exception as e:
            logger.error(f"Failed to process {table_name}: {str(e)}")
            # Continue with other tables
            continue
    
    logger.info("Bronze to Silver processing completed!")


if __name__ == "__main__":
    main()
