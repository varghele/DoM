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

def create_buckets_no_public_access(bucket_names, region):
    # Initialize the storage client
    client = storage.Client()

    for bucket_name in bucket_names:
        bucket = client.lookup_bucket(bucket_name)
        if bucket is None:
            # Create bucket without public access
            print(f"Creating bucket '{bucket_name}' in region '{region}' with no public access...")
            bucket = storage.Bucket(client, name=bucket_name)
            bucket = client.create_bucket(bucket, location=region)

            # Enable Uniform Bucket-Level Access
            bucket.iam_configuration.uniform_bucket_level_access_enabled = True
            bucket.patch()  # Apply the changes to the bucket

            print(f"Bucket '{bucket_name}' created successfully with no public access.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")


if __name__ == "__main__":
    create_buckets_no_public_access(BUCKET_NAMES, REGION)