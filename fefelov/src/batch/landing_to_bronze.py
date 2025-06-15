"""
Landing to Bronze - Download data from FTP and save as Parquet
Part 2, Step 1 of the Final Project
Updated for GoIT External Services with fefelov prefix
"""
import logging
import os
import sys
import requests
from typing import List

from pyspark.sql import SparkSession

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - fefelov_%(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def download_data(local_file_path: str, ftp_url: str = "https://ftp.goit.study/neoversity/") -> None:
    """
    Download CSV file from FTP server
    
    Args:
        local_file_path: Path where to save the downloaded file (without .csv extension)
        ftp_url: FTP server URL
    """
    downloading_url = ftp_url + os.path.basename(local_file_path) + ".csv"
    
    logger.info(f"Downloading from {downloading_url}")
    
    try:
        response = requests.get(downloading_url, timeout=60)
        
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_file_path + ".csv"), exist_ok=True)
            
            # Open the local file in write-binary mode and write the content
            with open(local_file_path + ".csv", 'wb') as file:
                file.write(response.content)
            logger.info(f"File downloaded successfully and saved as {local_file_path}.csv")
        else:
            raise Exception(f"Failed to download the file. Status code: {response.status_code}")
            
    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        raise


def process_landing_to_bronze(table_name: str, config: Config) -> None:
    """
    Process a single table from landing (CSV) to bronze (Parquet)
    
    Args:
        table_name: Name of the table to process
        config: Configuration object with paths and settings
    """
    logger.info(f"Processing {table_name} from landing to bronze...")
    
    # Initialize Spark session with prefixed app name
    spark = SparkSession.builder \
        .appName(f"{config.student_prefix}_LandingToBronze_{table_name}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Step 1: Download file from FTP server
        landing_dir = f"{config.data_path}/landing"
        os.makedirs(landing_dir, exist_ok=True)
        landing_file_path = f"{landing_dir}/{table_name}"
        download_data(landing_file_path, config.ftp_url)
        
        # Step 2: Read CSV file with Spark
        logger.info(f"Reading CSV file: {landing_file_path}.csv")
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"{landing_file_path}.csv")
        
        logger.info(f"Loaded {df.count()} records from {table_name}.csv")
        logger.info(f"Schema: {df.columns}")
        
        # Step 3: Save as Parquet in bronze layer with prefixed path
        bronze_path = f"{config.bronze_path}/{table_name}"
        logger.info(f"Saving to bronze layer: {bronze_path}")
        
        df.write \
            .mode("overwrite") \
            .parquet(bronze_path)
        
        logger.info(f"Successfully saved {table_name} to bronze layer")
        
        # Show sample data for verification
        logger.info("Sample data:")
        df.show(5, truncate=False)
        
    except Exception as e:
        logger.error(f"Error processing {table_name}: {str(e)}")
        raise
    finally:
        spark.stop()


def main() -> None:
    """
    Main function to process all tables from landing to bronze
    """
    logger.info("Starting Landing to Bronze processing with fefelov prefix...")
    
    # Load configuration
    config = Config()
    
    # List of tables to process
    tables = ["athlete_bio", "athlete_event_results"]
    
    # Create necessary directories
    os.makedirs(f"{config.data_path}/landing", exist_ok=True)
    os.makedirs(config.bronze_path, exist_ok=True)
    
    logger.info(f"Using student prefix: {config.student_prefix}")
    logger.info(f"Data path: {config.data_path}")
    logger.info(f"Bronze path: {config.bronze_path}")
    
    for table_name in tables:
        try:
            process_landing_to_bronze(table_name, config)
        except Exception as e:
            logger.error(f"Failed to process {table_name}: {str(e)}")
            # Continue with other tables
            continue
    
    logger.info("Landing to Bronze processing completed!")


if __name__ == "__main__":
    main()
