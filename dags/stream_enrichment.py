from pyspark.sql import SparkSession

# Створити Spark-сесію
spark = SparkSession.builder \
    .appName("StreamEnrichmentPipeline") \
    .config("spark.jars", "/opt/airflow/dags/jars/mysql-connector-j-8.0.32.jar") \
    .getOrCreate()

# Kafka параметри
kafka_server = "broker:9092"
topic = "enriched_events"

# MySQL параметри
mysql_server = "217.61.57.46"
db = "olympic_dataset"
table_name = "enriched_event_results"
jdbc_user = "airflow"
jdbc_password = "airflow"

# Прочитати потік з Kafka
event_stream_enriched = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", topic) \
    .load()

# Обробка кожної партії
def foreach_batch_function(batch_df, batch_id):
    # Відправка до Kafka (якщо потрібно)
    batch_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("topic", topic) \
        .save()

    # Запис до MySQL
    batch_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{mysql_server}:3306/{db}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .mode("append") \
        .save()

# Запустити стрімінг
event_stream_enriched \
    .writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .start() \
    .awaitTermination()
