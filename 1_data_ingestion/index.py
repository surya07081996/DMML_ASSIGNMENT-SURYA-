import os
import logging
import pandas as pd
import requests
import zipfile
import urllib3
from datetime import datetime

urllib3.disable_warnings()

# Configure logging
LOG_FILE = "1_data_ingestion/logs/churn_ingestion.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define file paths
DATA_DIR = "1_data_ingestion/data/"
RAW_DIR = "1_data_ingestion/raw_data/"
CSV_FILE = "Bank Customer Churn Prediction.csv"
API_URL = "https://maven-datasets.s3.amazonaws.com/Bank+Customer+Churn/Bank+Customer+Churn.zip"

# Ensure required directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)


def ingest_csv_data():
    """Ingest customer churn data from a CSV file."""
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, CSV_FILE))
        raw_path = os.path.join(RAW_DIR, f"churn_csv_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
        df.to_csv(raw_path, index=False)
        logging.info(f"Successfully ingested customer churn data from CSV. File saved at {raw_path}")
        print("CSV data ingestion successful.")
    except Exception as e:
        logging.error(f"Error ingesting CSV data: {e}")
        print("CSV data ingestion failed.")


def ingest_api_data():
    """Ingest customer churn data from an API (ZIP file)."""
    try:
        response = requests.get(API_URL, verify=False)
        if response.status_code == 200:
            zip_path = os.path.join(DATA_DIR, "Bank+Customer+Churn.zip")
            with open(zip_path, 'wb') as file:
                file.write(response.content)
            print('Download successful!')
        else:
            print('Failed to download the dataset. Status code:', response.status_code)
            return

        with zipfile.ZipFile(zip_path, 'r') as z:
            file_names = z.namelist()
            csv_file_name = file_names[0]
            with z.open(csv_file_name) as f:
                df = pd.read_csv(f)
                raw_path = os.path.join(RAW_DIR, f"churn_api_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
                df.to_csv(raw_path, index=False)
                logging.info(f"Successfully ingested customer churn data from API. File saved at {raw_path}")
                print("API data ingestion successful.")
    except Exception as e:
        logging.error(f"Error ingesting API data: {e}")
        print("API data ingestion failed.")


if __name__ == "__main__":
    ingest_csv_data()
    ingest_api_data()
    print("✅ Data ingestion complete!")