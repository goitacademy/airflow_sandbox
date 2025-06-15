"""
Spark session manager
"""
import logging
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from common.config import Config

logger = logging.getLogger(__name__)


class SparkManager:
    """Manages Spark session and common operations"""

    def __init__(self, config: Config):
        self.config = config
        self._spark: Optional[SparkSession] = None

    @property
    def spark(self) -> SparkSession:
        """Get or create Spark session"""
        if self._spark is None:
            self._spark = self._create_spark_session()
        return self._spark

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        logger.info("Creating Spark session...")
        
        # Windows compatibility - set HADOOP_HOME to avoid winutils error
        import os
        import platform
        if platform.system() == "Windows" and "HADOOP_HOME" not in os.environ:
            # Try to find a reasonable path or create a temp directory
            import tempfile
            hadoop_home = os.path.join(tempfile.gettempdir(), "hadoop")
            os.makedirs(hadoop_home, exist_ok=True)
            os.environ["HADOOP_HOME"] = hadoop_home
            logger.info(f"Set HADOOP_HOME to: {hadoop_home}")

        # Handle both local and Docker environments for JAR path
        jar_path = self.config.spark.mysql_jar_path

        builder = SparkSession.builder \
            .appName(self.config.spark.app_name) \
            .config("spark.jars", jar_path) \
            .config("spark.driver.extraClassPath", jar_path) \
            .config("spark.executor.extraClassPath", jar_path) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", self.config.checkpoint_location)

        # Windows-specific configurations for remote cluster
        if platform.system() == "Windows":
            builder = builder \
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
                .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
                .config("spark.driver.host", "localhost")

        # Set master URL if not running in cluster mode
        if self.config.spark.master:
            builder = builder.master(self.config.spark.master)

        # Add Kafka packages for external Kafka with SASL
        builder = builder.config("spark.jars.packages",
                                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        logger.info(f"Spark session created: {spark.version}")
        return spark

    def read_mysql_table(self, table_name: str) -> "DataFrame":
        """Read table from MySQL database"""
        logger.info(f"Reading MySQL table: {table_name}")

        return self.spark.read \
            .format("jdbc") \
            .option("url", self.config.database.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.config.database.username) \
            .option("password", self.config.database.password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()

    def write_to_mysql(self, df: "DataFrame", table_name: str, mode: str = "append") -> None:
        """Write DataFrame to MySQL table"""
        logger.info(f"Writing to MySQL table: {table_name} (mode: {mode})")

        df.write \
            .format("jdbc") \
            .option("url", self.config.database.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.config.database.username) \
            .option("password", self.config.database.password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode(mode) \
            .save()

    def read_kafka_stream(self, topic: str, schema: Optional[StructType] = None) -> "DataFrame":
        """Read stream from Kafka topic with authentication support"""
        logger.info(f"Reading Kafka stream from topic: {topic}")

        # Build Kafka options with authentication
        kafka_options = self.config.kafka.kafka_options.copy()
        kafka_options.update({
            "subscribe": topic,
            "startingOffsets": "latest"
        })

        # Create the stream reader
        stream_reader = self.spark.readStream.format("kafka")

        # Add all Kafka options
        for key, value in kafka_options.items():
            stream_reader = stream_reader.option(key, value)

        df = stream_reader.load()

        # Parse JSON value if schema provided
        if schema:
            from pyspark.sql.functions import from_json, col
            df = df.select(
                from_json(col("value").cast("string"), schema).alias("data")
            ).select("data.*")

        return df

    def write_to_kafka(self, df: "DataFrame", topic: str) -> None:
        """Write DataFrame to Kafka topic with authentication support"""
        logger.info(f"Writing to Kafka topic: {topic}")

        from pyspark.sql.functions import to_json, struct

        # Convert all columns to JSON
        json_df = df.select(
            to_json(struct(*df.columns)).alias("value")
        )

        # Build Kafka options with authentication
        kafka_options = self.config.kafka.kafka_options.copy()
        kafka_options["topic"] = topic

        # Create the writer
        writer = json_df.write.format("kafka")

        # Add all Kafka options
        for key, value in kafka_options.items():
            writer = writer.option(key, value)

        writer.save()

    def write_stream_to_kafka(self, df: "DataFrame", topic: str, checkpoint_location: str) -> "StreamingQuery":
        """Write streaming DataFrame to Kafka topic with authentication support"""
        logger.info(f"Writing stream to Kafka topic: {topic}")

        from pyspark.sql.functions import to_json, struct

        # Convert all columns to JSON
        json_df = df.select(
            to_json(struct(*df.columns)).alias("value")
        )

        # Build Kafka options with authentication
        kafka_options = self.config.kafka.kafka_options.copy()
        kafka_options["topic"] = topic

        # Create the stream writer
        writer = json_df.writeStream \
            .format("kafka") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_location)

        # Add all Kafka options
        for key, value in kafka_options.items():
            writer = writer.option(key, value)

        return writer.start()

    def read_parquet(self, path: str) -> "DataFrame":
        """Read Parquet files"""
        logger.info(f"Reading Parquet from: {path}")
        return self.spark.read.parquet(path)

    def write_parquet(self, df: "DataFrame", path: str, mode: str = "overwrite") -> None:
        """Write DataFrame as Parquet"""
        logger.info(f"Writing Parquet to: {path} (mode: {mode})")
        df.write.mode(mode).parquet(path)

    def stop(self) -> None:
        """Stop Spark session"""
        if self._spark:
            logger.info("Stopping Spark session...")
            self._spark.stop()
            self._spark = None
