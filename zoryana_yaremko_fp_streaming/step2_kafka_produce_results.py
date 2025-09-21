# Етап 2. Зчитування з MySQL → Kafka
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct

spark = SparkSession.builder \
    .appName("ProduceToKafka") \
    .config("spark.jars", "/opt/airflow/libs/mysql-connector-j-8.0.33.jar") \
    .getOrCreate()

df = spark.read.format("jdbc").options(
    url="jdbc:mysql://host.docker.internal:3306/olympic_dataset",
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="athlete_event_results",
    user="root",
    password="example"
).load()

df.selectExpr("CAST(athlete_id AS STRING) as key") \
  .withColumn("value", to_json(struct(*df.columns))) \
  .select("key", "value") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "broker:29092") \
  .option("topic", "athlete_event_results") \
  .save()

spark.stop()
