import functions_framework
from main import fetch_files
from flask import Request
import os
from google.cloud import storage


def test_function():
    # Create a mock request
    mock_request = Request.from_values()

    # Ensure you have credentials set up
    if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
        print("Please set GOOGLE_APPLICATION_CREDENTIALS environment variable")
        return

    # Test the function
    try:
        response = fetch_files(mock_request)
        print(f"Function response: {response}")
    except Exception as e:
        print(f"Error occurred: {str(e)}")


if __name__ == "__main__":
    test_function()
