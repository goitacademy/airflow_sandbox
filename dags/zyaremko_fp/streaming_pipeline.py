from pyspark.sql import SparkSession

if __name__ == "__main__":
    # створюємо SparkSession
    spark = SparkSession.builder \
        .appName("StreamingPipeline") \
        .getOrCreate()

    print("✅ Spark Streaming Pipeline стартував!")

    # приклад простого DataFrame (для тесту, потім замінимо на Kafka + MySQL)
    df = spark.createDataFrame(
        [(1, "Zoryana", 170, 60),
         (2, "Olena", 165, 55)],
        ["athlete_id", "name", "height", "weight"]
    )

    df.show()

    spark.stop()
