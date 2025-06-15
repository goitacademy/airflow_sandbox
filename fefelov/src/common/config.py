"""
Configuration management for the application
"""
import os
from dataclasses import dataclass
from typing import Optional, Dict, Any

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class DatabaseConfig:
    """Database configuration"""
    host: str
    port: int
    database: str
    username: str
    password: str
    
    @property
    def jdbc_url(self) -> str:
        return f"jdbc:mysql://{self.host}:{self.port}/{self.database}"


@dataclass
class KafkaConfig:
    """Kafka configuration with authentication support"""
    bootstrap_servers: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    
    @property
    def servers_list(self) -> list[str]:
        return self.bootstrap_servers.split(",")
    
    @property
    def kafka_options(self) -> Dict[str, Any]:
        """Get Kafka options for Spark Structured Streaming"""
        options = {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "kafka.security.protocol": self.security_protocol,
        }
        
        if self.security_protocol == "SASL_PLAINTEXT" and self.sasl_mechanism:
            options.update({
                "kafka.sasl.mechanism": self.sasl_mechanism,
                "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{self.sasl_username}" password="{self.sasl_password}";'
            })
        
        return options


@dataclass
class TopicsConfig:
    """Kafka topics configuration with prefix support"""
    prefix: str = ""
    athlete_events: str = "athlete_event_results"
    enriched_avg: str = "athlete_enriched_avg"
    
    @property
    def prefixed_athlete_events(self) -> str:
        return f"{self.prefix}_{self.athlete_events}" if self.prefix else self.athlete_events
    
    @property
    def prefixed_enriched_avg(self) -> str:
        return f"{self.prefix}_{self.enriched_avg}" if self.prefix else self.enriched_avg


@dataclass
class TablesConfig:
    """Database tables configuration with prefix support"""
    # Input tables (no prefix - existing data)
    athlete_bio: str = "athlete_bio"
    athlete_event_results: str = "athlete_event_results"
    # Configuration fields
    prefix: str = ""
    enriched_avg: str = "athlete_enriched_avg"
    bronze: str = "bronze_athletes"
    silver: str = "silver_athletes"
    gold: str = "gold_athletes"
    
    @property
    def prefixed_enriched_avg(self) -> str:
        return f"{self.prefix}_{self.enriched_avg}" if self.prefix else self.enriched_avg
    
    @property
    def prefixed_bronze(self) -> str:
        return f"{self.prefix}_{self.bronze}" if self.prefix else self.bronze
    
    @property
    def prefixed_silver(self) -> str:
        return f"{self.prefix}_{self.silver}" if self.prefix else self.silver
    
    @property
    def prefixed_gold(self) -> str:
        return f"{self.prefix}_{self.gold}" if self.prefix else self.gold


@dataclass
class SparkConfig:
    """Spark configuration"""
    app_name: str
    master: str
    mysql_jar_path: str


@dataclass
class Config:
    """Main application configuration"""
    
    def __init__(self):        # Student prefix for resource naming
        self.student_prefix = os.getenv("STUDENT_PREFIX", "fefelov")
        
        # Database configurations
        # Source database for reading Olympic data
        self.database = DatabaseConfig(
            host=os.getenv("MYSQL_HOST", "217.61.57.46"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            database=os.getenv("MYSQL_DATABASE", "olympic_dataset"),
            username=os.getenv("MYSQL_USERNAME", "neo_data_admin"),
            password=os.getenv("MYSQL_PASSWORD", "Proyahaxuqithab9oplp")
        )
        
        # Target database for writing streaming results        # Target database configuration - Real MySQL database
        self.target_database = DatabaseConfig(
            host=os.getenv("MYSQL_TARGET_HOST", "217.61.57.46"),
            port=int(os.getenv("MYSQL_TARGET_PORT", "3306")),
            database=os.getenv("MYSQL_TARGET_DATABASE", "neo_data"),
            username=os.getenv("MYSQL_TARGET_USERNAME", "neo_data_admin"),
            password=os.getenv("MYSQL_TARGET_PASSWORD", "Proyahaxuqithab9oplp")
        )
        
        # Kafka configuration with authentication
        self.kafka = KafkaConfig(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "77.81.230.104:9092"),
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            sasl_username=os.getenv("KAFKA_SASL_USERNAME", "admin"),
            sasl_password=os.getenv("KAFKA_SASL_PASSWORD", "VawEzo1ikLtrA8Ug8THa")
        )
        # Topics configuration (use actual prefixed values from env vars)
        self.topics = TopicsConfig(
            prefix=self.student_prefix,
            athlete_events=os.getenv("KAFKA_TOPIC_ATHLETE_EVENTS", f"{self.student_prefix}_athlete_event_results"),
            enriched_avg=os.getenv("KAFKA_TOPIC_ENRICHED_AGG", f"{self.student_prefix}_athlete_enriched_avg")
        )
        
        # Tables configuration (use actual prefixed values from env vars)
        self.tables = TablesConfig(
            prefix=self.student_prefix,
            enriched_avg=os.getenv("MYSQL_TABLE_ENRICHED_AGG", f"{self.student_prefix}_athlete_enriched_avg"),
            bronze=os.getenv("MYSQL_TABLE_BRONZE", f"{self.student_prefix}_bronze_athletes"),
            silver=os.getenv("MYSQL_TABLE_SILVER", f"{self.student_prefix}_silver_athletes"),
            gold=os.getenv("MYSQL_TABLE_GOLD", f"{self.student_prefix}_gold_athletes")
        )
        
        # Spark configuration
        self.spark = SparkConfig(
            app_name=os.getenv("SPARK_APP_NAME", f"{self.student_prefix}_goit_de_final_project"),
            master=os.getenv("SPARK_MASTER_URL", "spark://217.61.58.159:7077"),
            mysql_jar_path=os.getenv("MYSQL_JAR_PATH", "./jars/mysql-connector-j-8.0.32.jar")
        )
        
        # Data paths
        self.data_path = os.getenv("DATA_PATH", "./data")
        self.bronze_path = f"{self.data_path}/bronze"
        self.silver_path = f"{self.data_path}/silver"
        self.gold_path = f"{self.data_path}/gold"
        self.checkpoint_location = os.getenv("CHECKPOINT_LOCATION", f"./data/checkpoints/{self.student_prefix}")
        self.output_path = os.getenv("OUTPUT_PATH", f"./data/output/{self.student_prefix}")
        
        # FTP configuration
        self.ftp_url = os.getenv("FTP_URL", "https://ftp.goit.study/neoversity/")
        
        # Airflow configuration
        self.airflow_user = os.getenv("AIRFLOW_USER", "airflow")
        self.airflow_password = os.getenv("AIRFLOW_PASSWORD", "airflow")
        self.airflow_host = os.getenv("AIRFLOW_HOST", "https://airflow.goit.global")
        
        # DAG configuration
        self.dag_id_prefix = os.getenv("DAG_ID_PREFIX", self.student_prefix)
        self.dag_batch_pipeline = os.getenv("DAG_BATCH_PIPELINE", f"{self.student_prefix}_batch_pipeline")
        self.dag_streaming_setup = os.getenv("DAG_STREAMING_SETUP", f"{self.student_prefix}_streaming_setup")
    
    def get_mysql_properties(self) -> dict:
        """Get MySQL connection properties for Spark"""
        return {
            "user": self.database.username,
            "password": self.database.password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }
