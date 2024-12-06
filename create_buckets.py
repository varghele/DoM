from google.cloud import storage

# List of bucket names to ensure exist
BUCKET_NAMES = ["doppelm-detr-pretrade",
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
REGION = "EU"  # Specify the region

def ensure_buckets_exist(bucket_names, region):
    # Initialize the storage client
    client = storage.Client()

    for bucket_name in bucket_names:
        bucket = client.lookup_bucket(bucket_name)
        if bucket is None:
            # If the bucket doesn't exist, create it
            print(f"Bucket '{bucket_name}' does not exist. Creating it in the '{region}' region...")
            bucket = client.create_bucket(bucket_name, location=region)
            print(f"Bucket '{bucket_name}' created successfully in the '{region}' region.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")

if __name__ == "__main__":
    ensure_buckets_exist(BUCKET_NAMES, REGION)
