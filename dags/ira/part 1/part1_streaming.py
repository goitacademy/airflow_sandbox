import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, avg,
    current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, FloatType, DoubleType
)


os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3,'
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'
)

BOOTSTRAP_SERVERS = "77.81.230.104:9092"
KAFKA_USERNAME    = "admin"
KAFKA_PASSWORD    = "VawEzo1ikLtrA8Ug8THa"
SASL_CONFIG = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
)

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers":    BOOTSTRAP_SERVERS,
    "kafka.security.protocol":    "SASL_PLAINTEXT",
    "kafka.sasl.mechanism":       "PLAIN",
    "kafka.sasl.jaas.config":     SASL_CONFIG,
}

MY_NAME               = "ira"
TOPIC_INPUT           = "athlete_event_results"
TOPIC_OUTPUT          = f"athlete_enriched_{MY_NAME}"
OUTPUT_TABLE          = f"athlete_enriched_{MY_NAME}"
CHECKPOINT_KAFKA      = f"/tmp/fp_checkpoint_kafka_{MY_NAME}"
CHECKPOINT_DB         = f"/tmp/fp_checkpoint_db_{MY_NAME}"

MYSQL_HOST     = "217.61.57.46"
MYSQL_PORT     = "3306"
MYSQL_DB       = "olympic_dataset"
MYSQL_USER     = "neo_data_admin"
MYSQL_PASSWORD = "Proyahaxuqithab9oplp"
MYSQL_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?useSSL=false&allowPublicKeyRetrieval=true"
MYSQL_PROPS    = {"user": MYSQL_USER, "password": MYSQL_PASSWORD, "driver": "com.mysql.cj.jdbc.Driver"}


spark = SparkSession.builder \
    .appName("FP Part 1 - Streaming Pipeline") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# Stage 1. Read athlete_bio from MySQL
athlete_bio_df = spark.read \
    .jdbc(url=MYSQL_URL, table="athlete_bio", properties=MYSQL_PROPS)

# Stage 2. Filter out rows where height or weight are empty or non-numeric
athlete_bio_df = spark.read \
    .jdbc(url=MYSQL_URL, table="athlete_bio", properties=MYSQL_PROPS)
 
# Stage 2. Filter out rows where height or weight are empty or non-numeric
athlete_bio_df = athlete_bio_df \
    .filter(col("height").isNotNull() & col("weight").isNotNull()) \
    .filter(col("height").cast("double").isNotNull()) \
    .filter(col("weight").cast("double").isNotNull()) \
    .withColumn("height", col("height").cast("double")) \
    .withColumn("weight", col("weight").cast("double"))
 
print("=== Stage 1+2: athlete_bio (filtered) ===")
athlete_bio_df.show(5)


# Stage 3a. Read athlete_event_results from MySQL → write to Kafka
event_results_df = spark.read \
    .jdbc(url=MYSQL_URL, table="athlete_event_results", properties=MYSQL_PROPS)
 
# Filter before writing to Kafka
event_results_df = event_results_df \
    .filter(
        col("athlete_id").isNotNull() &
        col("sport").isNotNull() &
        col("country_noc").isNotNull()
    )
 
event_results_df \
    .select(to_json(struct("*")).alias("value")) \
    .write \
    .format("kafka") \
    .options(**KAFKA_OPTIONS) \
    .option("topic", TOPIC_INPUT) \
    .option("checkpointLocation", f"/tmp/fp_write_kafka_{MY_NAME}") \
    .save()
 
print(f"=== Stage 3a: filtered athlete_event_results written to Kafka topic '{TOPIC_INPUT}' ===")


# Stage 3b. Read athlete_event_results from Kafka → parse JSON
event_schema = StructType([
    StructField("edition",     StringType(),  True),
    StructField("edition_id",  IntegerType(), True),
    StructField("country_noc", StringType(),  True),
    StructField("sport",       StringType(),  True),
    StructField("event",       StringType(),  True),
    StructField("result_id",   IntegerType(), True),
    StructField("athlete",     StringType(),  True),
    StructField("athlete_id",  IntegerType(), True),
    StructField("pos",         StringType(),  True),
    StructField("medal",       StringType(),  True),
    StructField("isTeamSport", StringType(),  True),
])
 
kafka_stream = spark.readStream \
    .format("kafka") \
    .options(**KAFKA_OPTIONS) \
    .option("subscribe", TOPIC_INPUT) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "50000") \
    .option("kafka.session.timeout.ms", "120000") \
    .option("kafka.request.timeout.ms", "120000") \
    .load()
 
event_stream = kafka_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), event_schema).alias("data")) \
    .select("data.*")


# Stage 4. Join with athlete_bio on athlete_id
joined_stream = event_stream.join(
    athlete_bio_df,
    on="athlete_id",
    how="inner"
).drop(athlete_bio_df["country_noc"])

# Stage 5. Avg height & weight per sport/medal/sex/country_noc + timestamp
aggregated_stream = joined_stream \
    .groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
    ) \
    .withColumn("timestamp", current_timestamp())


# Stage 6. forEachBatch → write to Kafka + MySQL
def process_batch(batch_df, batch_id):
    batch_df.persist()
    print(f"=== Batch {batch_id} ===")
    batch_df.show(10, truncate=False)
 
    # Stage 6a. Write to output Kafka topic
    batch_df \
        .select(to_json(struct("*")).alias("value")) \
        .write \
        .format("kafka") \
        .options(**KAFKA_OPTIONS) \
        .option("topic", TOPIC_OUTPUT) \
        .save()
    print(f"Stage 6a: batch {batch_id} written to Kafka topic '{TOPIC_OUTPUT}'")
 
    # Stage 6b. Write to MySQL
    batch_df.write \
        .jdbc(
            url=MYSQL_URL,
            table=OUTPUT_TABLE,
            mode="append",
            properties=MYSQL_PROPS,
        )
    print(f"Stage 6b: batch {batch_id} written to MySQL table '{OUTPUT_TABLE}'")
 
    batch_df.unpersist()
 
query = aggregated_stream.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("complete") \
    .option("checkpointLocation", CHECKPOINT_DB) \
    .trigger(availableNow=True) \
    .start()
 
print(f"Streaming to Kafka topic '{TOPIC_OUTPUT}' and MySQL table '{OUTPUT_TABLE}'...")
query.awaitTermination()
