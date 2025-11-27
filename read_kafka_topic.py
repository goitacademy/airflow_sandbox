# Для запуску файлу виконати команду spark-submit \
#  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
#  read_kafka_topic.py

# Читання повідомлень з Kafka-топіку enriched_athlete_stats

from pyspark.sql import SparkSession

KAFKA_SERVER = "77.81.230.104:9092"
OUTPUT_TOPIC = "enriched_athlete_stats"

KAFKA_USERNAME = "admin"
KAFKA_PASSWORD = "VawEzo1ikLtrA8Ug8THa"

KAFKA_JAAS_CONFIG = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
)

spark = SparkSession.builder.appName("ReadKafkaTopic").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", OUTPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", KAFKA_JAAS_CONFIG)
    .load()
)

print(">>> Повідомлення з Kafka-топіку enriched_athlete_stats:")
df.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value").show(
    20, truncate=False
)

spark.stop()
