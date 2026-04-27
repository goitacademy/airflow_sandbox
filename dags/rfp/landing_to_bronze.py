import requests
import os

DAG_DIR = os.path.dirname(os.path.abspath(__file__))

table_names = ['athlete_event_results', 'athlete_bio']

def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    path = DAG_DIR + "/" + local_file_path + ".csv"

    if response.status_code == 200:
        with open(path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")

def save_file_to_bronze(local_file_path):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName('Landing To Bronze').getOrCreate()

    try:
        csv_path = os.path.join(DAG_DIR, local_file_path + '.csv')
        full_path = os.path.join(DAG_DIR, 'bronze', local_file_path)

        df = (spark.read
              .option('header', True)
              .option('inferSchema', True)
              .csv(csv_path))

        (df.write
         .mode('overwrite')
         .parquet(full_path))
        print(f"File saved to {full_path}")

    finally:
        spark.stop()
        local_path = os.path.join(DAG_DIR, local_file_path + '.csv')
        if os.path.exists(local_path):
            os.remove(local_path)

def main():
    for table_name in table_names:
        download_data(table_name)
        save_file_to_bronze(table_name)

if __name__ == "__main__":
    main()