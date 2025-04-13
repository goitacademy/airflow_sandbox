import requests
import os

from pyspark.sql import SparkSession


# Створюємо сесію Spark
spark = SparkSession.builder.appName("BronzeLevel").getOrCreate()


def download_data(output_folder, local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)
    # print(response.status_code)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the local file in write-binary mode and write the content of the response to it
        with open(output_folder+'/'+local_file_path+".csv", 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}.csv")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")


output_folder = 'bronze'

file_name_list = [
                    'athlete_bio',
                    'athlete_event_results',
                ]

# Створення папки лише якщо вона не існує
os.makedirs(output_folder, exist_ok=True)

for file in file_name_list:
    download_data(output_folder, file)
    temp_df = spark.read.csv(f'{output_folder}/{file}.csv', header=True)
    temp_df.write.format("parquet").option("compression", "gzip").mode("overwrite").save(f'{output_folder}/{file}')
    print(f'Saved {output_folder}/{file}.parquet')
    temp_df.show()
