# Етап 3. Kafka → Join → Kafka + MySQL
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("StreamKafkaJoin") \
    .config("spark.jars", "/opt/airflow/libs/mysql-connector-j-8.0.33.jar") \
    .getOrCreate()

# Біо
df_bio = spark.read.parquet("data/bronze/athlete_bio")

# Схема для Kafka JSON
schema = StructType([
    StructField("athlete_id", IntegerType()),
    StructField("sport", StringType()),
    StructField("medal", StringType()),
    StructField("country_noc", StringType()),
])

# Kafka stream
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "athlete_event_results") \
    .load()

df_parsed = df_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Join
df_joined = df_parsed.join(df_bio, on="athlete_id")

# Аггрегація
df_agg = df_joined.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ).withColumn("calc_timestamp", current_timestamp())

# Write to Kafka
df_agg.selectExpr("CAST(sport AS STRING) as key", "to_json(struct(*)) as value") \
    .writeStream \
    .outputMode("complete") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("topic", "avg_stats_results") \
    .option("checkpointLocation", "chk_avg_stats") \
    .start()

# Write to MySQL
def write_to_mysql(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://host.docker.internal:3306/olympic_dataset") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "avg_stats") \
        .option("user", "root") \
        .option("password", "example") \
        .mode("overwrite") \
        .save()

df_agg.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_mysql) \
    .option("checkpointLocation", "chk_mysql") \
    .start() \
    .awaitTermination()
