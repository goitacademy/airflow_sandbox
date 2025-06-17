from pyspark.sql import SparkSession

# Налаштування параметрів підключення до MySQL
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table = "athlete_event_results"
jdbc_user = "airflow"     
jdbc_password = "airflow"  

# Створення SparkSession
spark = SparkSession.builder \
    .appName("LoadAthletesFromMySQL") \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .getOrCreate()

# Завантаження таблиці з MySQL у Spark DataFrame
athletes_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", jdbc_table) \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .load()

# Показати перші рядки
athletes_df.show(truncate=False)

# Опціонально: зупинити Spark-сесію, якщо скрипт standalone
spark.stop()
