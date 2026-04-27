import requests
import os

def download_data(file_name: str):
    url = f"https://ftp.goit.study/neoversity/{file_name}.csv"
    local_path = f"landing/{file_name}.csv"
    os.makedirs("landing", exist_ok=True)
    print(f"Downloading {url}")
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(response.content)
        print(f"{file_name} successfully saved in {local_path}")
    else:
        raise Exception(f"{file_name} failed to download")