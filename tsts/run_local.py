import functions_framework
from main import fetch_files

if __name__ == "__main__":
    # Start the local development server
    functions_framework.start(target=fetch_files)
