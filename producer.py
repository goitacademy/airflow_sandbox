# producer.py - для одноразового завантаження даних у Kafka

# Для запуску файлу виконати команду spark-submit \
#  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
#  --jars mysql-connector-j-8.0.32.jar \
#  producer.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
from pyspark.sql.types import StringType

# --- КОНФІГУРАЦІЯ  ---
# Налаштування MySQL
MYSQL_HOST = "217.61.57.46"
MYSQL_PORT = "3306"
MYSQL_DB = "olympic_dataset"
MYSQL_USER = "neo_data_admin"
MYSQL_PASSWORD = "Proyahaxuqithab9oplp"
JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
JDBC_JAR = "mysql-connector-j-8.0.32.jar"

# Налаштування Kafka
KAFKA_SERVER = "77.81.230.104:9092"
INPUT_TOPIC = "athlete_event_results"

KAFKA_USERNAME = "admin"
KAFKA_PASSWORD = "VawEzo1ikLtrA8Ug8THa"

KAFKA_JAAS_CONFIG = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
)


def run_producer():
    print(">>> Створення Spark сесії для Kafka Producer...")

    # Створення Spark сесії з JDBC драйвером
    spark = (
        SparkSession.builder.appName("KafkaProducer")
        .config("spark.jars", JDBC_JAR)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # 1. Читання даних з MySQL таблиці athlete_event_results
    print(">>> Читання даних athlete_event_results з MySQL...")
    df_results = (
        spark.read.format("jdbc")
        .options(
            url=JDBC_URL,
            driver=JDBC_DRIVER,
            dbtable="athlete_event_results",
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
        )
        .load()
    )

    df_results.cache()
    print(f"Кількість записів для відправки: {df_results.count()}")

    # 2. Конвертація в JSON та запис у Kafka
    print(f">>> Запис даних у Kafka-топік '{INPUT_TOPIC}'...")

    kafka_df = (
        df_results.selectExpr("CAST(athlete_id AS STRING) AS key")
        .withColumn("value", to_json(struct("*")))
        .selectExpr(
            "CAST(key AS STRING) AS key",
            "CAST(value AS STRING) AS value",
        )
    )

    (
        kafka_df.write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("topic", INPUT_TOPIC)
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", KAFKA_JAAS_CONFIG)
        .mode("append")
        .save()
    )

    print(">>> Запис у Kafka завершено успішно.")
    spark.stop()


if __name__ == "__main__":
    run_producer()
