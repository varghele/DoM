import subprocess
import sys

# Ensure `pytz` is installed
try:
    import pytz
except ImportError:
    print("The 'pytz' module is not installed. Attempting to install it...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pytz"])
    print("Installation complete. Resuming script execution...")
    import pytz  # Retry importing pytz after installation

import requests
from urllib.parse import urlparse
from google.cloud import storage
from datetime import datetime, timedelta
import pytz

# Define constants
EUROPEAN_TIMEZONE = pytz.timezone("Europe/Berlin")
BASE_URL = "https://mifid2-apa-data.deutsche-boerse.com"
BUCKET_NAMES = [
    "doppelm-detr-pretrade",
    "doppelm-detr-posttrade",
    "doppelm-dfra-pretrade",
    "doppelm-dfra-posttrade",
    "doppelm-dxsc-pretrade",
    "doppelm-dxsc-posttrade",
    "doppelm-dgat-pretrade",
    "doppelm-dgat-posttrade",
    "doppelm-deex-pretrade",
    "doppelm-deex-posttrade",
    "doppelm-dpow-pretrade",
    "doppelm-dpow-posttrade",
    "doppelm-deur-pretrade-mdoptions",
    "doppelm-deur-pretrade-others",
    "doppelm-deur-posttrade",
]
DICT_EXT_BUCKET_NAMES = {
    "doppelm-detr-pretrade": "DETR-pretrade",
    "doppelm-detr-posttrade": "DETR-posttrade",
    "doppelm-dfra-pretrade": "DFRA-pretrade",
    "doppelm-dfra-posttrade": "DFRA-posttrade",
    "doppelm-dxsc-pretrade": "DXSC-pretrade",
    "doppelm-dxsc-posttrade": "DXSC-posttrade",
    "doppelm-dgat-pretrade": "DGAT-pretrade",
    "doppelm-dgat-posttrade": "DGAT-posttrade",
    "doppelm-deex-pretrade": "DEEX-pretrade",
    "doppelm-deex-posttrade": "DEEX-posttrade",
    "doppelm-dpow-pretrade": "DPOW-pretrade",
    "doppelm-dpow-posttrade": "DPOW-posttrade",
    "doppelm-deur-pretrade-mdoptions": "DEUR-pretradeMDOptions",
    "doppelm-deur-pretrade-others": "DEUR-pretradeOthers",
    "doppelm-deur-posttrade": "DEUR-posttrade",
}

def download_file_and_upload_to_gcs(bucket_name, file_name, url):
    """
    Download a file from a URL and upload it to a GCS bucket.
    """
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Raise exception for HTTP errors
        data = response.content
    except requests.RequestException as e:
        print(f"Failed to download {url}: {e}")
        return

    try:
        upload_to_gcs(bucket_name, file_name, data)
    except Exception as e:
        print(f"Failed to upload {file_name} to bucket {bucket_name}: {e}")


def upload_to_gcs(bucket_name, file_name, data):
    """
    Upload a file to a GCS bucket.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(data, content_type="application/json")
    print(f"Uploaded '{file_name}' to bucket '{bucket_name}'.")


def file_exists_in_bucket(bucket_name, file_name):
    """
    Check if a file exists in a GCS bucket.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    return blob.exists()


def generate_timestamps(hours=12):
    """
    Generate a list of timestamps for every minute in the past `hours`.
    """
    now = datetime.now(EUROPEAN_TIMEZONE)
    start_time = now - timedelta(hours=hours)
    return [
        (start_time + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M")
        for i in range(int((now - start_time).total_seconds() // 60) + 1)
    ]


if __name__ == "__main__":
    timestamps = generate_timestamps()

    for bucket in BUCKET_NAMES:
        ext_name = DICT_EXT_BUCKET_NAMES[bucket]
        for timestamp in timestamps:
            # Construct download URL and file name
            file_name = f"{ext_name}-{timestamp}.json.gz"
            download_url = f"{BASE_URL}/{ext_name}/{file_name}"

            # Check if file already exists in the bucket
            if not file_exists_in_bucket(bucket, file_name):
                print(f"Processing: {file_name}")
                download_file_and_upload_to_gcs(bucket, file_name, download_url)
            else:
                print(f"File '{file_name}' already exists in bucket '{bucket}'. Skipping...")
