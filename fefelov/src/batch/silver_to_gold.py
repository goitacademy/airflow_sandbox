"""
Silver to Gold - Join data and create analytical dataset
Part 2, Step 3 of the Final Project
Updated for GoIT External Services with fefelov prefix
"""
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, current_timestamp, col, lit
from pyspark.sql.types import DoubleType

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - fefelov_%(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def read_silver_tables(spark: SparkSession, config: Config) -> tuple[DataFrame, DataFrame]:
    """
    Read athlete_bio and athlete_event_results from silver layer
    
    Args:
        spark: Spark session
        config: Configuration object with paths
        
    Returns:
        Tuple of (athlete_bio_df, athlete_event_results_df)
    """
    logger.info("Reading tables from silver layer...")
    
    # Read athlete_bio
    bio_path = f"{config.silver_path}/athlete_bio"
    bio_df = spark.read.parquet(bio_path)
    logger.info(f"Loaded {bio_df.count()} records from athlete_bio")
    
    # Read athlete_event_results
    results_path = f"{config.silver_path}/athlete_event_results"
    results_df = spark.read.parquet(results_path)
    logger.info(f"Loaded {results_df.count()} records from athlete_event_results")
    
    return bio_df, results_df


def prepare_data_for_join(bio_df: DataFrame, results_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Prepare data for joining by ensuring proper data types
    
    Args:
        bio_df: Athlete bio DataFrame
        results_df: Athlete event results DataFrame
        
    Returns:
        Tuple of prepared DataFrames
    """
    logger.info("Preparing data for join...")
    
    # Ensure weight and height are numeric (Double type)
    bio_prepared = bio_df.withColumn("height", col("height").cast(DoubleType())) \
                         .withColumn("weight", col("weight").cast(DoubleType()))
    
    # Filter out records with null or invalid weight/height
    bio_prepared = bio_prepared.filter(
        col("height").isNotNull() & 
        col("weight").isNotNull() &
        (col("height") > 0) & 
        (col("weight") > 0)
    )
    
    logger.info(f"After filtering invalid records: {bio_prepared.count()} bio records")
    
    return bio_prepared, results_df


def join_and_aggregate(bio_df: DataFrame, results_df: DataFrame) -> DataFrame:
    """
    Join tables and calculate aggregated statistics
    
    Args:
        bio_df: Athlete bio DataFrame
        results_df: Athlete event results DataFrame
        
    Returns:
        Aggregated DataFrame with average stats
    """
    logger.info("Joining tables on athlete_id...")
    
    # Create table aliases to avoid column ambiguity
    bio_alias = bio_df.alias("bio")
    results_alias = results_df.alias("results")
    
    # Join on athlete_id using explicit table aliases
    joined_df = results_alias.join(bio_alias, col("results.athlete_id") == col("bio.athlete_id"), "inner")
    logger.info(f"Joined dataset contains {joined_df.count()} records")
    
    # Group by sport, medal, sex, country_noc and calculate averages
    # Use results table's country_noc since it's the main fact table
    logger.info("Calculating average weight and height by grouping...")
    
    agg_df = joined_df.groupBy(
                          col("results.sport"), 
                          col("results.medal"), 
                          col("bio.sex"), 
                          col("results.country_noc")
                      ) \
                      .agg(
                          avg(col("bio.weight")).alias("avg_weight"),
                          avg(col("bio.height")).alias("avg_height")
                      ) \
                      .withColumn("timestamp", current_timestamp())
    
    # Round averages to 2 decimal places for better readability
    agg_df = agg_df.withColumn("avg_weight", col("avg_weight").cast("decimal(10,2)")) \
                   .withColumn("avg_height", col("avg_height").cast("decimal(10,2)"))
    
    # Rename columns to remove table prefixes for cleaner output
    agg_df = agg_df.select(
        col("sport").alias("sport"),
        col("medal").alias("medal"), 
        col("sex").alias("sex"),
        col("country_noc").alias("country_noc"),
        col("avg_weight").alias("avg_weight"),
        col("avg_height").alias("avg_height"),
        col("timestamp").alias("timestamp")
    )
    
    logger.info(f"Calculated aggregated statistics for {agg_df.count()} groups")
    
    return agg_df


def process_silver_to_gold(config: Config) -> None:
    """
    Process data from silver to gold layer
    
    Args:
        config: Configuration object with paths and settings
    """
    logger.info("Processing from silver to gold layer...")
    
    # Initialize Spark session with prefixed app name
    spark = SparkSession.builder \
        .appName(f"{config.student_prefix}_SilverToGold_AvgStats") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Step 1: Read tables from silver layer
        bio_df, results_df = read_silver_tables(spark, config)
        
        # Step 2: Prepare data for joining
        bio_prepared, results_prepared = prepare_data_for_join(bio_df, results_df)
        
        # Step 3: Join and aggregate data
        gold_df = join_and_aggregate(bio_prepared, results_prepared)
        
        # Step 4: Save to gold layer
        gold_path = f"{config.gold_path}/avg_stats"
        logger.info(f"Saving aggregated data to gold layer: {gold_path}")
        
        gold_df.write \
            .mode("overwrite") \
            .parquet(gold_path)
        
        logger.info("Successfully saved aggregated data to gold layer")
        
        # Show sample data for verification
        logger.info("Sample aggregated data:")
        gold_df.show(10, truncate=False)
        
        # Show summary statistics
        logger.info("Summary statistics:")
        logger.info(f"Total groups: {gold_df.count()}")
        logger.info(f"Unique sports: {gold_df.select('sport').distinct().count()}")
        logger.info(f"Unique countries: {gold_df.select('country_noc').distinct().count()}")
        
    except Exception as e:
        logger.error(f"Error processing silver to gold: {str(e)}")
        raise
    finally:
        spark.stop()


def main() -> None:
    """
    Main function to process data from silver to gold layer
    """
    logger.info("Starting Silver to Gold processing with fefelov prefix...")
    
    # Load configuration
    config = Config()
    
    # Create gold directory
    os.makedirs(config.gold_path, exist_ok=True)
    
    logger.info(f"Using student prefix: {config.student_prefix}")
    logger.info(f"Silver path: {config.silver_path}")
    logger.info(f"Gold path: {config.gold_path}")
    
    try:
        process_silver_to_gold(config)
        logger.info("Silver to Gold processing completed successfully!")
    except Exception as e:
        logger.error(f"Silver to Gold processing failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
