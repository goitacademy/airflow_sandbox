# ===============================
# Імпорти Airflow та Spark
# ===============================
from airflow import DAG
from airflow.operators.python import PythonOperator  # <-- Головне виправлення №1
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType


# ===============================
# Етап 0. Визначення функції для Airflow
# ===============================
#
# ВЕСЬ ВАШ КОД тепер знаходиться всередині цієї функції.
# Airflow не буде запускати його при імпорті, а викличе
# його ТІЛЬКИ тоді, коли настане час виконати завдання.
#
def run_streaming_pipeline_job():
    print("🚀 [Airflow] Запускаємо Spark-завдання...")

    try:
        # ===============================
        # Етап 1. SparkSession + MySQL
        # ===============================
        spark = (
            SparkSession.builder.appName("EndToEndStreamingPipeline").config(
                "spark.jars.packages", "mysql:mysql-connector-java:8.0.33"
            )
            # ^-- Головне виправлення №2:
            # Замість невірного локального шляху, ми кажемо Spark
            # автоматично завантажити правильний драйвер.
            # Це виправить помилку "ClassNotFoundException".
            .getOrCreate()
        )

        print("✅ [Spark] Spark сесія створена")

        jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
        jdbc_user = "neo_data_admin"
        jdbc_password = "Proyahaxuqithab9oplp"

        athlete_bio_df = (
            spark.read.format("jdbc")
            .options(
                url=jdbc_url,
                driver="com.mysql.cj.jdbc.Driver",
                dbtable="athlete_bio",
                user=jdbc_user,
                password=jdbc_password,
            )
            .load()
        )

        print("✅ [Spark] Етап 1: Біо-дані завантажено")

        # ===============================
        # Етап 2. Фільтрація біо-даних
        # ===============================
        athlete_bio_df_clean = athlete_bio_df.filter(
            col("height").cast("int").isNotNull()
        ).filter(col("weight").cast("int").isNotNull())

        print("✅ [Spark] Етап 2: Біо-дані відфільтровано")

        # ===============================
        # Етап 3. Дані з Kafka
        # ===============================
        # УВАГА: 'localhost:9092' може не спрацювати, якщо Kafka
        # запущена не на тій же машині, що й Airflow worker.
        # Можливо, знадобиться вказати IP-адресу сервера Kafka.
        kafka_server = "localhost:9092"
        input_topic = "athlete_event_results"

        event_schema = StructType(
            [
                StructField("event_id", StringType()),
                StructField("athlete_id", StringType()),
                StructField("sport", StringType()),
                StructField("medal", StringType()),
                StructField("year", StringType()),
            ]
        )

        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_server)
            .option("subscribe", input_topic)
            .option("startingOffsets", "latest")
            .load()
        )

        event_df = (
            kafka_df.selectExpr("CAST(value AS STRING) as json_str")
            .select(from_json(col("json_str"), event_schema).alias("data"))
            .select("data.*")
        )

        print("✅ [Spark] Етап 3: Дані з Kafka зчитано")

        # ===============================
        # Етап 4. Join
        # ===============================
        joined_df = event_df.join(athlete_bio_df_clean, on="athlete_id", how="inner")

        print("✅ [Spark] Етап 4: Join виконано")

        # ===============================
        # Етап 5. Агрегація
        # ===============================
        aggregated_df = (
            joined_df.groupBy("sport", "medal", "sex", "country_noc")
            .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight"))
            .withColumn("calculated_at", current_timestamp())
        )

        print("✅ [Spark] Етап 5: Агрегація виконана")

        # ===============================
        # Етап 6. Sink у Kafka + MySQL
        # ===============================
        def write_to_sinks(batch_df, batch_id):
            print(f"--- [Spark] Обробка batch {batch_id} ---")
            # 6a. Kafka
            batch_df.selectExpr(
                "to_json(named_struct('sport', sport, 'medal', medal, 'sex', sex, "
                "'country_noc', country_noc, 'avg_height', avg_height, "
                "'avg_weight', avg_weight, 'calculated_at', calculated_at)) AS value"
            ).write.format("kafka").option(
                "kafka.bootstrap.servers", kafka_server
            ).option(
                "topic", "aggregated_athlete_stats"
            ).save()

            # 6b. MySQL
            batch_df.write.format("jdbc").option("url", jdbc_url).option(
                "driver", "com.mysql.cj.jdbc.Driver"
            ).option("dbtable", "aggregated_athlete_stats").option(
                "user", jdbc_user
            ).option(
                "password", jdbc_password
            ).mode(
                "append"
            ).save()
            print(f"--- [Spark] Batch {batch_id} успішно записано ---")

        print("✅ [Spark] Етап 6: Sink функція визначена")

        # ===============================
        # Запуск стріму
        # ===============================
        query = (
            aggregated_df.writeStream.foreachBatch(write_to_sinks)
            .outputMode("update")
            .option("checkpointLocation", "/tmp/spark_checkpoints")
            .start()
        )

        print("🚀 [Spark] Потік запущено... Завдання Airflow завершується.")
        #
        # query.awaitTermination() # <-- Головне виправлення №3:
        # Ми ВИДАЛИЛИ .awaitTermination(), тому що Airflow-завдання
        # не повинно "зависати" назавжди. Воно має запустити
        # потік у фоновому режимі і завершитись.
        #

    except Exception as e:
        print(f"❌ [Spark] ПОМИЛКА під час виконання: {e}")
        raise


# ===============================
# Етап 7. Визначення DAG
# ===============================
#
# Тепер сам DAG-файл - це лише ОПИС.
# Він не виконує жодної роботи, лише каже Airflow:
# "Будь ласка, створи DAG з ОДНИМ завданням,
# яке запускає функцію run_streaming_pipeline_job".
#
with DAG(
    "streaming_pipeline_FIXED",  # Я додав _FIXED до назви
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "streaming", "fix"],
) as dag:

    run_spark_job = PythonOperator(
        task_id="run_spark_streaming_job",
        python_callable=run_streaming_pipeline_job,  # <-- Вказуємо нашу функцію
    )
