"""
Kafka Spark Streaming Pipeline for Part 1 of the Final Project

This pipeline implements the requirements:
1. Read athlete bio data from MySQL olympic_dataset.athlete_bio table
2. Filter records with empty or non-numeric height/weight data  
3. Read athlete_event_results from MySQL and write to Kafka topic
4. Read event results from Kafka stream and parse JSON to DataFrame
5. Join Kafka stream with bio data using athlete_id
6. Calculate average height/weight by sport, medal, sex, country_noc with timestamp
7. Stream results using forEachBatch to both Kafka topic and MySQL database
"""
import logging
import sys
import os
from datetime import datetime
from typing import Optional

# Add parent directories to Python path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)  # fefelov/src
grandparent_dir = os.path.dirname(parent_dir)  # fefelov
sys.path.append(parent_dir)
sys.path.append(grandparent_dir)

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, avg, current_timestamp, 
    from_json, to_json, struct, 
    isnan, isnull, when, coalesce, lit,
    regexp_replace, trim, cast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, DoubleType, TimestampType
)

# Import common modules
from common.config import Config
from common.spark_manager import SparkManager
from common.utils import validate_required_columns

logger = logging.getLogger(__name__)


class KafkaSparkStreamingPipeline:
    """
    End-to-End Streaming Pipeline for Olympic Data Processing
    
    Implements all requirements from Part 1:
    1. Read athlete bio data from MySQL olympic_dataset.athlete_bio 
    2. Filter invalid height/weight data
    3. Read athlete_event_results from MySQL and write to Kafka
    4. Read from Kafka stream and parse JSON to DataFrame
    5. Join streams on athlete_id
    6. Calculate aggregated statistics (avg height/weight by sport, medal, sex, country)
    7. Stream results using forEachBatch to Kafka and MySQL
    """
    
    def __init__(self, spark_manager: SparkManager, config: Config):
        self.spark_manager = spark_manager
        self.config = config
        
        # Schema for athlete event results (flexible to match actual data)
        self.athlete_event_schema = StructType([
            StructField("athlete_id", IntegerType(), True),
            StructField("sport", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("country_noc", StringType(), True),
            StructField("edition", StringType(), True),
            StructField("edition_id", IntegerType(), True),
            StructField("event", StringType(), True),
            StructField("result_id", IntegerType(), True),
            StructField("games_year", IntegerType(), True),            StructField("season", StringType(), True),
            StructField("city", StringType(), True)
        ])
    
    def load_athlete_bio_data(self) -> DataFrame:
        """        Requirement 1 & 2: Load athlete bio data from MySQL and filter invalid records
        
        - Read from olympic_dataset.athlete_bio table
        - Filter out empty or non-numeric height/weight data
        """
        logger.info("Step 1: Loading athlete bio data from olympic_dataset.athlete_bio...")
        
        try:
            # Read athlete bio data from olympic_dataset.athlete_bio (source table without prefix)
            bio_df = self.spark_manager.read_mysql_table(self.config.tables.athlete_bio)
            
            logger.info("Bio data schema:")
            bio_df.printSchema()
            
            initial_count = bio_df.count()
            logger.info(f"Loaded {initial_count} athlete bio records")
            
            if initial_count > 0:
                logger.info("Sample bio data:")
                bio_df.show(3, truncate=False)
            
            # Convert string height/weight to numeric and clean data
            logger.info("Converting height/weight from string to numeric...")
            # Clean and convert height/weight to numeric values
            cleaned_bio_df = bio_df \
                .withColumn("height_numeric", cast(regexp_replace(trim(col("height")), "[^0-9.]", ""), "double")) \
                .withColumn("weight_numeric", 
                    cast(regexp_replace(trim(col("weight")), "[^0-9.]", ""), "double"))
            
            # Step 2: Filter out records with invalid height/weight data
            logger.info("Step 2: Filtering out invalid height/weight data...")
            filtered_bio_df = cleaned_bio_df.filter(
                col("height_numeric").isNotNull() & 
                col("weight_numeric").isNotNull() &
                ~isnan(col("height_numeric")) & 
                ~isnan(col("weight_numeric")) &
                (col("height_numeric") > 0) & 
                (col("weight_numeric") > 0)
            ).select(
                col("athlete_id"),
                col("name"),
                col("sex"),
                col("country_noc"),
                col("height_numeric").alias("height"),
                col("weight_numeric").alias("weight")
            )
            
            filtered_count = filtered_bio_df.count()
            logger.info(f"After filtering: {filtered_count} valid athlete bio records")
            logger.info(f"Filtered out {initial_count - filtered_count} invalid records")
            
            if filtered_count > 0:
                logger.info("Sample filtered bio data:")
                filtered_bio_df.show(3, truncate=False)
            
            return filtered_bio_df
            
        except Exception as e:
            logger.error(f"Error loading athlete bio data: {e}")
            # Create a mock bio dataframe for testing if table doesn't exist
            logger.warning("Creating mock bio data for testing...")
            return self._create_mock_bio_data()
    
    def _create_mock_bio_data(self) -> DataFrame:
        """Create mock bio data for testing when table doesn't exist"""
        mock_data = [
            (1, "Michael Phelps", "M", 1985, 193.0, 91.0, "USA"),
            (2, "Katie Ledecky", "F", 1997, 183.0, 70.0, "USA"),
            (3, "Usain Bolt", "M", 1986, 195.0, 94.0, "JAM"),
            (4, "Simone Biles", "F", 1997, 142.0, 47.0, "USA"),
            (5, "Eliud Kipchoge", "M", 1984, 167.0, 52.0, "KEN")
        ]
        
        return self.spark_manager.spark.createDataFrame(
            mock_data, 
            ["athlete_id", "name", "sex", "birth_year", "height", "weight", "country_noc"]
        )
    def setup_kafka_data_producer(self) -> None:
        """
        Requirement 3: Read athlete_event_results from MySQL and write to Kafka topic
        Using prefixed topic name for multi-user environment
        """
        logger.info("Step 3: Reading athlete_event_results from MySQL and writing to Kafka...")
        
        try:
            # Read athlete event results from MySQL (source table without prefix)
            event_results_df = self.spark_manager.read_mysql_table(self.config.tables.athlete_event_results)
            
            logger.info("Event results schema:")
            event_results_df.printSchema()
            
            event_count = event_results_df.count()
            logger.info(f"Loaded {event_count} athlete event results")
            
            if event_count > 0:
                logger.info("Sample event results:")
                event_results_df.show(3, truncate=False)
                
                # Write to Kafka topic with prefixed name
                kafka_topic = self.config.topics.athlete_events
                logger.info(f"Writing to Kafka topic: {kafka_topic}")
                self.spark_manager.write_to_kafka(event_results_df, kafka_topic)
                logger.info("Successfully wrote athlete event results to Kafka topic")
            else:
                logger.warning("No event results found, writing mock data to Kafka...")
                self._write_mock_event_results_to_kafka()
                
        except Exception as e:
            logger.error(f"Error setting up Kafka producer: {e}")
            logger.warning("Writing mock event results to Kafka for testing...")
            self._write_mock_event_results_to_kafka()
    
    def _write_mock_event_results_to_kafka(self) -> None:
        """Write mock event results to Kafka for testing with prefixed topic"""
        mock_events = [
            (1, "Swimming", "GOLD", "USA", "2016 Summer Olympics", 2016, "100m Freestyle", 1, 2016, "Summer", "Rio"),
            (2, "Swimming", "GOLD", "USA", "2016 Summer Olympics", 2016, "400m Freestyle", 2, 2016, "Summer", "Rio"),
            (3, "Athletics", "GOLD", "JAM", "2016 Summer Olympics", 2016, "100m Sprint", 3, 2016, "Summer", "Rio"),
            (4, "Gymnastics", "GOLD", "USA", "2016 Summer Olympics", 2016, "All-Around", 4, 2016, "Summer", "Rio"),
            (5, "Athletics", "GOLD", "KEN", "2016 Summer Olympics", 2016, "Marathon", 5, 2016, "Summer", "Rio"),
            (1, "Swimming", "SILVER", "USA", "2012 Summer Olympics", 2012, "200m Freestyle", 6, 2012, "Summer", "London"),
            (3, "Athletics", "BRONZE", "JAM", "2012 Summer Olympics", 2012, "200m Sprint", 7, 2012, "Summer", "London")
        ]
        
        mock_df = self.spark_manager.spark.createDataFrame(
            mock_events,
            ["athlete_id", "sport", "medal", "country_noc", "edition", 
            "edition_id", "event", "result_id", "games_year", "season", "city"]
        )
        
        kafka_topic = self.config.topics.athlete_events
        logger.info(f"Writing mock data to Kafka topic: {kafka_topic}")
        self.spark_manager.write_to_kafka(mock_df, kafka_topic)
        logger.info("Mock event results written to Kafka")
    
    def read_kafka_stream(self) -> DataFrame:
        """
        Requirement 4: Read athlete event results from Kafka stream and parse JSON to DataFrame
        Using prefixed topic name for multi-user environment
        """
        logger.info("Step 4: Reading athlete event results from Kafka stream...")
        
        # Read from Kafka with prefixed topic name and parse JSON to DataFrame format
        kafka_topic = self.config.topics.athlete_events
        logger.info(f"Reading from Kafka topic: {kafka_topic}")
        kafka_df = self.spark_manager.read_kafka_stream(
            kafka_topic, 
            self.athlete_event_schema
        )
        logger.info("Successfully set up Kafka stream reader")
        return kafka_df
    
    def join_streams(self, bio_df: DataFrame, kafka_df: DataFrame) -> DataFrame:
        """
        Requirement 5: Join Kafka stream with MySQL bio data using athlete_id
        """
        logger.info("Step 5: Joining Kafka stream with athlete bio data using athlete_id...")
        
        # Validate that both DataFrames have athlete_id column
        if "athlete_id" not in bio_df.columns:
            raise ValueError("Bio DataFrame missing athlete_id column")
        
        if "athlete_id" not in kafka_df.columns:
            raise ValueError("Kafka DataFrame missing athlete_id column")
        
        # Alias the DataFrames to avoid column ambiguity
        bio_aliased = bio_df.alias("bio")
        kafka_aliased = kafka_df.alias("events")
        
        # Join on athlete_id (inner join to only include matching records)
        # Select specific columns to avoid ambiguity, preferring country_noc from events table
        joined_df = kafka_aliased.join(bio_aliased, "athlete_id", "inner").select(
            col("events.athlete_id"),
            col("events.sport"),
            col("events.medal"),
            col("events.country_noc"),  # Use country_noc from events table
            col("bio.name"),
            col("bio.sex"),
            col("bio.height"),
            col("bio.weight")
        )
        
        logger.info("Successfully joined streams on athlete_id")
        return joined_df
    
    def calculate_aggregated_stats(self, joined_df: DataFrame) -> DataFrame:
        """
        Requirement 6: Calculate average height/weight by sport, medal type, sex, country_noc
        Add timestamp when calculations were made
        """
        logger.info("Step 6: Calculating aggregated statistics...")
        
        # Handle missing medal values (athletes who didn't win medals)
        # Replace null medals with "No Medal" for proper grouping
        cleaned_df = joined_df.withColumn(
            "medal_status", 
            when(col("medal").isNull() | (col("medal") == ""), "No Medal")
            .otherwise(col("medal"))
        )
        
        # Group by sport, medal status, sex, and country_noc
        # Calculate average height and weight with timestamp
        agg_df = cleaned_df.groupBy(
            "sport",
            "medal_status",
            "sex",
            "country_noc"
        ).agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight"),
            current_timestamp().alias("calculation_timestamp")
        )

        logger.info("Calculated aggregated statistics")
        return agg_df 
    
    
    def foreach_batch_function(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Requirement 7: Process each micro-batch using forEachBatch
        Stream results to both Kafka topic and MySQL database
        Using prefixed names for multi-user environment
        """
        logger.info(f"Processing batch {batch_id}...")

        if batch_df.count() == 0:
            logger.info(f"Batch {batch_id} is empty, skipping...")
            return

        try:
            # Log batch sample data
            self._log_batch_data(batch_df, batch_id)

            # Requirement 7a: Write to output Kafka topic with prefix
            enriched_topic = self.config.topics.enriched_avg
            logger.info(
                f"Writing aggregated results to Kafka topic: {enriched_topic}")
            self.spark_manager.write_to_kafka(batch_df, enriched_topic)

            # Requirement 7b: Write to MySQL database with prefixed table name
            enriched_table = self.config.tables.enriched_avg
            logger.info(
                f"Writing aggregated results to MySQL table: {enriched_table}")
            self.spark_manager.write_to_mysql(
                batch_df,
                enriched_table,
                mode="append"
            )
            
            logger.info(f"Batch {batch_id} processed successfully!")
            
        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {str(e)}")
            raise
    
    def _log_batch_data(self, batch_df: DataFrame, batch_id: int) -> None:        # Show sample data for verification
        logger.info(f"Batch {batch_id} contains {batch_df.count()} records. Sample data:")
        batch_df.show(5, truncate=False)
    
    def run(self) -> None:
        """
        Run the complete streaming pipeline implementing all requirements
        """
        logger.info("🚀 Starting Kafka Spark Streaming Pipeline for Olympic Data...")
        logger.info("Implementing Part 1 requirements:")
        logger.info("1. Read athlete bio data from MySQL olympic_dataset.athlete_bio")
        logger.info("2. Filter invalid height/weight data")
        logger.info("3. Read athlete_event_results from MySQL and write to Kafka")
        logger.info("4. Read from Kafka stream and parse JSON to DataFrame")
        logger.info("5. Join streams on athlete_id") 
        logger.info("6. Calculate avg height/weight by sport, medal, sex, country")
        logger.info("7. Stream results using forEachBatch to Kafka and MySQL")
        
        try:
            # Requirements 1 & 2: Load and filter athlete bio data
            bio_df = self.load_athlete_bio_data()
            
            # Requirement 3: Setup Kafka producer (write MySQL data to Kafka)
            self.setup_kafka_data_producer()
            
            # Give Kafka some time to receive the data
            logger.info("Waiting 5 seconds for Kafka to receive data...")
            import time
            time.sleep(5)
            
            # Requirement 4: Read from Kafka stream
            kafka_stream_df = self.read_kafka_stream()
            
            # Requirement 5: Join streams on athlete_id
            joined_df = self.join_streams(bio_df, kafka_stream_df)
            
            # Requirement 6: Calculate aggregated statistics
            enriched_stream = self.calculate_aggregated_stats(joined_df)
            # Requirement 7: Setup streaming query with forEachBatch
            logger.info("Setting up streaming query with forEachBatch...")
            
            # Use prefixed checkpoint location
            checkpoint_path = f"{self.config.checkpoint_location}/streaming"
            logger.info(f"Using checkpoint location: {checkpoint_path}")
            
            query = enriched_stream \
                .writeStream \
                .foreachBatch(self.foreach_batch_function) \
                .outputMode("complete") \
                .option("checkpointLocation", checkpoint_path) \
                .trigger(processingTime="30 seconds") \
                .start()
            
            logger.info("✅ Streaming pipeline started successfully!")
            logger.info("Pipeline is now processing data every 30 seconds...")
            logger.info("Press Ctrl+C to stop the pipeline...")
            
            # Wait for termination
            query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("Pipeline stopped by user")
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {str(e)}")
            raise
        finally:
            logger.info("Stopping Spark session...")
            self.spark_manager.stop()


if __name__ == "__main__":
    # For standalone execution - set up paths
    import sys
    import os
    
    # Add parent directories to path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)  # fefelov/src
    grandparent_dir = os.path.dirname(parent_dir)  # fefelov
    sys.path.append(parent_dir)
    sys.path.append(grandparent_dir)
    
    from common.config import Config
    from common.spark_manager import SparkManager
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    config = Config()
    spark_manager = SparkManager(config)
    
    pipeline = KafkaSparkStreamingPipeline(spark_manager, config)
    pipeline.run()
