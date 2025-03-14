from datetime import datetime
import logging
import os
import pandas as pd

# Configure logging
logging.basicConfig(filename='1_data_ingestion/logs/churn_ingestion.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define file paths
DATA_DIR = "1_data_ingestion/data/"
RAW_DIR = "1_data_ingestion/raw_data/"

CSV = "Bank Customer Churn Prediction.csv"

def ingest_csv_data():
    try:
        # Read CSV file
        df = pd.read_csv(os.path.join(DATA_DIR, CSV))

        # Save a copy in raw format with a timestamp
        raw_path = os.path.join(RAW_DIR, f"churn_csv_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
        df.to_csv(raw_path, index=False)

        logging.info(f"Successfully ingested customer churn data from CSV. File saved at {raw_path}")
        print("CSV data ingestion successful.")

    except Exception as e:
        logging.error(f"Error ingesting CSV data: {e}")
        print("CSV data ingestion failed.")