import datetime
from google.cloud import storage


def test_bucket_access():
    """Test if we can access/create buckets"""
    storage_client = storage.Client()

    try:
        # Try to list buckets to test credentials
        buckets = list(storage_client.list_buckets(max_results=1))
        print("Successfully connected to GCP")
        return True
    except Exception as e:
        print(f"Error connecting to GCP: {str(e)}")
        return False


def test_timestamp_generation():
    """Test timestamp generation"""
    from main import generate_timestamps

    timestamps = generate_timestamps(hours=1)  # Test with 1 hour for faster results
    print(f"Generated {len(timestamps)} timestamps")
    print(f"Sample timestamps: {timestamps[:5]}")


if __name__ == "__main__":
    print("Testing GCP connection...")
    test_bucket_access()

    print("\nTesting timestamp generation...")
    test_timestamp_generation()
