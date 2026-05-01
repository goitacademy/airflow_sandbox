import requests
import os


def download_data(file_name: str, landing_dir: str):
    url = f"https://ftp.goit.study/neoversity/{file_name}.csv"

    os.makedirs(landing_dir, exist_ok=True)

    local_path = os.path.join(landing_dir, f"{file_name}.csv")

    print(f"Downloading {url}")

    response = requests.get(url)

    if response.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(response.content)
        print(f"{file_name} successfully saved in {local_path}")
    else:
        raise Exception(f"{file_name} failed to download. Status code: {response.status_code}")