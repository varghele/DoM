from main import DICT_EXT_BUCKET_NAMES, BASE_URL
import requests

def test_url_accessibility():
    """Test if the base URL is accessible"""
    for bucket_name, ext_name in DICT_EXT_BUCKET_NAMES.items():
        print(f"Testing access for {bucket_name}")
        test_url = f"{BASE_URL}/{ext_name}"
        try:
            response = requests.head(test_url)
            print(f"Status: {response.status_code}")
        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == "__main__":
    test_url_accessibility()
