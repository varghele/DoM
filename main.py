from google.cloud import storage
import datetime
from datetime import timedelta
import pytz
import functions_framework
import logging
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EUROPEAN_TIMEZONE = pytz.timezone('Europe/Berlin')
BASE_URL = "https://mfs.deutsche-boerse.com/api/download"

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


def generate_timestamps(hours=2):
    """
    Generate a list of timestamps for every minute in the past `hours`.
    """
    now = datetime.datetime.now(EUROPEAN_TIMEZONE)
    start_time = now - timedelta(hours=hours)
    return [
        (start_time + timedelta(minutes=i)).strftime("%Y-%m-%dT%H_%M")
        for i in range(int((now - start_time).total_seconds() // 60) + 1)
    ]


def fetch_and_store_files(bucket, start_time, end_time):
    """
    Fetch files from API and store in GCS bucket, checking for existing files first
    """
    bucket_name = bucket.name
    ext_name = DICT_EXT_BUCKET_NAMES[bucket_name]
    logger.info(f"Processing {bucket_name} with ext_name {ext_name}")

    timestamps = generate_timestamps()
    files_processed = 0
    files_skipped = 0

    for timestamp in timestamps:
        try:
            file_name = f"{ext_name}-{timestamp}.json.gz"

            # Check if file already exists in bucket
            blob = bucket.blob(file_name)
            if blob.exists():
                logger.info(f"File {file_name} already exists in {bucket_name}, skipping...")
                files_skipped += 1
                continue

            # If file doesn't exist, proceed with download
            download_url = f"{BASE_URL}/{file_name}"
            logger.info(f"Attempting to download: {download_url}")

            # Make the request
            response = requests.get(download_url)

            if response.status_code == 200:
                # Upload the file content
                blob.upload_from_string(response.content)
                logger.info(f"Successfully uploaded {file_name} to {bucket_name}")
                files_processed += 1
            else:
                logger.warning(f"Failed to download {file_name}: Status {response.status_code}")

        except Exception as e:
            logger.error(f"Error processing file {file_name}: {str(e)}")
            continue

    logger.info(f"Bucket {bucket_name} processing complete. "
                f"Files processed: {files_processed}, "
                f"Files skipped: {files_skipped}")


@functions_framework.http
def fetch_files(request):
    storage_client = storage.Client()

    # Set up Berlin timezone
    now = datetime.datetime.now(EUROPEAN_TIMEZONE)
    twelve_hours_ago = now - timedelta(hours=12)

    logger.info(f"Starting file fetch at Berlin time: {now}")

    # Create or access buckets
    buckets = {}
    for bucket_name in DICT_EXT_BUCKET_NAMES.keys():
        try:
            try:
                bucket = storage_client.get_bucket(bucket_name)
                logger.info(f"Using existing bucket: {bucket_name}")
            except Exception:
                logger.info(f"Creating new bucket: {bucket_name}")
                bucket = storage_client.create_bucket(
                    bucket_name,
                    location="europe-west3",
                    storage_class="STANDARD"
                )
                logger.info(f"Successfully created bucket: {bucket_name}")

            buckets[bucket_name] = bucket

        except Exception as e:
            logger.error(f"Error with bucket {bucket_name}: {str(e)}")
            return f"Error: Could not access/create bucket {bucket_name}", 500

    # Process each bucket
    for bucket_name, bucket in buckets.items():
        try:
            logger.info(f"Processing bucket: {bucket_name}")
            fetch_and_store_files(bucket, twelve_hours_ago, now)
        except Exception as e:
            logger.error(f"Error processing bucket {bucket_name}: {str(e)}")
            continue

    return "Function executed successfully"
