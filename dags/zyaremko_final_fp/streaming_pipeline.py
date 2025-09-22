# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ========== CONFIG ==========
MYSQL_URL = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
MYSQL_USER = "neo_data_admin"
MYSQL_PASS = "Proyahaxuqithab9o9lp"

KAFKA_SERVER = "77.81.230.104:9092"
KAFKA_TOPIC_IN = "athlete_event_results"
KAFKA_TOPIC_OUT = "athlete_enriched_agg"

# ========== SCHEMAS ==========
event_schema = StructType([
    StructField("athlete_id", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
])

# ========== SPARK SESSION ==========
spark = (
    SparkSession.builder
    .appName("StreamingPipeline")
    .config("spark.jars", "mysql-connector-j-8.0.32.jar")
    .getOrCreate()
)

# ========== STEP 1: Read athletes bio from MySQL ==========
bio_df = (
    spark.read.format("jdbc")
    .option("url", MYSQL_URL)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "athlete_bio")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASS)
    .load()
)

# Filter out empty/non-numeric height/weight
bio_df = (
    bio_df.filter(col("height").rlike("^[0-9]+$"))
          .filter(col("weight").rlike("^[0-9]+$"))
          .withColumn("height", col("height").cast(DoubleType()))
          .withColumn("weight", col("weight").cast(DoubleType()))
)

# ========== STEP 2: Read event results from Kafka ==========
raw_events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC_IN)
    .option("startingOffsets", "latest")
    .load()
)

events = (
    raw_events.selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), event_schema).alias("data"))
    .select("data.*")
)

# ========== STEP 3: Join events with bio ==========
enriched = events.join(bio_df, on="athlete_id", how="inner")

# ========== STEP 4: Aggregations ==========
agg = (
    enriched.groupBy("sport", "medal", "sex", "country_noc")
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    )
    .withColumn("timestamp", current_timestamp())
)

# ========== STEP 5: foreachBatch sink ==========
def foreach_batch_function(batch_df, batch_id):
    # Write back to Kafka
    batch_df.selectExpr("to_json(struct(*)) as value") \
        .write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("topic", KAFKA_TOPIC_OUT) \
        .save()

    # Write to MySQL
    batch_df.write.format("jdbc") \
        .option("url", MYSQL_URL) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "athlete_enriched_agg") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASS) \
        .mode("append") \
        .save()

query = (
    agg.writeStream
    .outputMode("update")
    .foreachBatch(foreach_batch_function)
    .start()
)

query.awaitTermination()
