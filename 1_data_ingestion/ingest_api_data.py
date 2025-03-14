from datetime import datetime
import logging
import os
import pandas as pd
import requests
import zipfile
import urllib3
urllib3.disable_warnings()

# Configure logging
logging.basicConfig(filename='1_data_ingestion/logs/churn_ingestion.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define file paths
DATA_DIR = "1_data_ingestion/data/"
RAW_DIR = "1_data_ingestion/raw_data/"

API_URL = "https://maven-datasets.s3.amazonaws.com/Bank+Customer+Churn/Bank+Customer+Churn.zip"

def ingest_api_data():
    try:
        response = requests.get(API_URL, verify=False)
        # Check if the request was successful
        if response.status_code == 200:
            # Write the content to a file
            with open(os.path.join(DATA_DIR, "Bank+Customer+Churn.zip"), 'wb') as file:
                file.write(response.content)
            print('Download successful!')
        else:
            print('Failed to download the data set. Status code:', response.status_code)

        # Open the ZIP file
        with zipfile.ZipFile(os.path.join(DATA_DIR, "Bank+Customer+Churn.zip"), 'r') as z:
            # List all files in the ZIP
            file_names = z.namelist()
            # For simplicity, we'll assume there's only one file and it's a CSV
            csv_file_name = file_names[0]

            # Read the file into a DataFrame
            with z.open(csv_file_name) as f:
                df = pd.read_csv(f)

                # Save a copy in raw format with a timestamp
                raw_path = os.path.join(RAW_DIR, f"churn_api_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
                df.to_csv(raw_path, index=False)

                logging.info(f"Successfully ingested customer churn data from API. File saved at {raw_path}")
                print("API data ingestion successful.")

    except Exception as e:
        logging.error(f"Error ingesting API data: {e}")
        print("API data ingestion failed.")