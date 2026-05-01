import os
import requests

def download_data(table, output_dir):
    url = f"https://ftp.goit.study/neoversity/{table}.csv"

    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f"{table}.csv")

    print(f"Downloading {url}")

    response = requests.get(url)
    response.raise_for_status()

    with open(output_path, "wb") as file:
        file.write(response.content)

    print(f"{table} successfully saved in {output_path}")