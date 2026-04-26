import requests
import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Landing To Bronze').getOrCreate()

table_names = ['athlete_event_results', 'athlete_bio']

def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    if response.status_code == 200:
        with open(local_file_path + ".csv", 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")

def save_file_to_bronze(local_file_path):
    full_path = f'bronze/{local_file_path}'
    df = (spark.read
          .option('header', True)
          .option('inferSchema', True)
          .csv(local_file_path + '.csv'))

    (df.write
     .mode('overwrite')
     .parquet(full_path)
     )

    if os.path.exists(local_file_path + '.csv'):
        os.remove(local_file_path + '.csv')

    print(f"File saved to {full_path}")

def main():
    for table_name in table_names:
        download_data(table_name)
        save_file_to_bronze(table_name)

if __name__ == "__main__":
    main()