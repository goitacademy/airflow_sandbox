# Етап 1. Зчитування з MySQL
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadMySQL") \
    .getOrCreate()

df = spark.read.format("jdbc").options(
    url="jdbc:mysql://host.docker.internal:3306/olympic_dataset",
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="athlete_bio",
    user="root",
    password="example"
).load()

df = df.filter(df.height.isNotNull() & df.weight.isNotNull())

df.write.mode("overwrite").parquet("data/bronze/athlete_bio")

spark.stop()
