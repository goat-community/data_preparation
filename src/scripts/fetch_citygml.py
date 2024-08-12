import os
import zipfile

import requests

from src.core.config import settings

# Supply BASE_URL with one of the following:
# Nordrhein-Westfalen: https://www.opengeodata.nrw.de/produkte/geobasis/3dg/lod2_gml/lod2_gml

BASE_URL = ""
JSON_MANIFEST_FILENAME = "index.json"

RESULT_DATA_DIR = os.path.join(settings.INPUT_DATA_DIR, "building_data")
RESULT_ZIP_FILENAME = "building_data.zip"


def fetch_json_manifest():
    # Fetch the JSON data from the URL
    print("Fetching JSON manifest...")
    response = requests.get(f"{BASE_URL}/{JSON_MANIFEST_FILENAME}")
    response.raise_for_status()
    return response.json()


def extract_citygml_filenames(json_manifest):
    # Extract the filenames from the JSON data
    return [file["name"] for file in json_manifest["datasets"][0]["files"]]


def fetch_citygml_files(citygml_filenames):
    # Fetch the CityGML files
    os.makedirs(RESULT_DATA_DIR, exist_ok=True)
    for i in range(len(citygml_filenames)):
        filename = citygml_filenames[i]
        print(f"Downloading {i + 1}/{len(citygml_filenames)}: {filename}")
        response = requests.get(f"{BASE_URL}/{filename}")
        response.raise_for_status()
        with open(os.path.join(RESULT_DATA_DIR, filename), "wb") as file:
            file.write(response.content)


def zip_citygml_files(citygml_filenames):
    # Zip the CityGML files
    print("Zipping CityGML files...")
    with zipfile.ZipFile(os.path.join(RESULT_DATA_DIR, RESULT_ZIP_FILENAME), 'w') as zipf:
        for filename in citygml_filenames:
            zipf.write(os.path.join(RESULT_DATA_DIR, filename))


def main():
    json_manifest = fetch_json_manifest()
    citygml_filenames = extract_citygml_filenames(json_manifest)
    fetch_citygml_files(citygml_filenames)
    zip_citygml_files(citygml_filenames)
    print("Done!")


if __name__ == '__main__':
    main()
