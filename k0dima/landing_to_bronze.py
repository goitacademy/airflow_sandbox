from pyspark.sql import SparkSession
import requests




def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the local file in write-binary mode and write the content of the response to it
        with open(f"{local_file_path}.csv", 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        exit(
            f"Failed to download the file. Status code: {response.status_code}")


tables = ["athlete_bio", "athlete_event_results"]
spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()


for table in tables:
    download_data(table)
    try:
        csv_path = f"{table}.csv"
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
    except Exception as err:
        print(f"Failed to read CSV file from {csv_path}. \nError: {err}")
        continue
    
    try:
        output_path = f"/tmp/bronze/{table}"
        df.write.mode("overwrite").parquet(output_path)
        print(f"Parquet file successfully saved to {output_path}") 
    except Exception as err:
        print(f"Failed to save Parquet file to {output_path}. \nError: {err}")
        
        
spark.stop()