import os
import pandas as pd
import shutil
from datetime import datetime

# Local storage config
RAW_DATA_DIR="1_data_ingestion/raw_data"
DATA_LAKE_DIR = "2_raw_data_storage/data_lake/raw"

def store_locally():
    try:
        files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith(".csv")]
        for file in files:
            file_path = os.path.join(RAW_DATA_DIR, file)
            df = pd.read_csv(file_path)

            # Basic validation
            if df.empty:
                print(f"Skipping empty file: {file}")
                continue

            """Stores raw data in a local data lake partitioned by date"""
            today = datetime.now().strftime("%Y/%m/%d")
            local_dir = os.path.join(DATA_LAKE_DIR, "bank_customer", today)
            os.makedirs(local_dir, exist_ok=True)

            destination_path = os.path.join(local_dir, os.path.basename(file_path))
            shutil.copy(file_path, destination_path)

            print(f"Stored {file_path} in {destination_path}")

    except Exception as e:
        print(f"Error storing raw data files: {e}")

store_locally()
