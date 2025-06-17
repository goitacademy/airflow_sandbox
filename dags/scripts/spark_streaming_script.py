from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Конфігурації
kafka_bootstrap_servers = "217.61.57.46:9092"  
input_topic = "input_olympic_topic"
output_topic = "output_olympic_topic"

jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "enriched_athlete_results"
jdbc_user = "airflow"
jdbc_password = "airflow"
mysql_driver = "com.mysql.cj.jdbc.Driver"

spark = SparkSession.builder \
    .appName("OlympicStreamingJob") \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .getOrCreate()

# Читаємо стрім з Kafka
input_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Десеріалізуємо value у рядок
df = input_stream.selectExpr("CAST(value AS STRING) as value")

# Тут можна додати будь-яку обробку, наприклад, просто додаємо колонку довжини рядка
from pyspark.sql.functions import length
processed_df = df.withColumn("value_length", length(col("value")))

def foreach_batch_function(batch_df, batch_id):
    # Відправка у Kafka (вихідний топік)
    batch_df.selectExpr("CAST(value AS STRING) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_topic) \
        .save()

    # Запис у MySQL
    batch_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", mysql_driver) \
        .option("dbtable", jdbc_table) \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .mode("append") \
        .save()

# Запускаємо стрімінг з foreachBatch
query = processed_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .start()

query.awaitTermination()

spark.stop()
