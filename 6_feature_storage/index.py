import sqlite3
import pandas as pd
import datetime

# Define SQLite database
DB_PATH = "feature_store.db"

# Load dataset from local storage
DATA_PATH = "1_data_ingestion/data/transformed_data.csv"
df = pd.read_csv(DATA_PATH)

# Identify categorical columns (assuming object type as categorical)
# Convert categorical columns to numerical (one-hot encoding)
categorical_cols = df.select_dtypes(include=["object"]).columns.tolist()
df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)

# Connect to SQLite
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Create Feature Metadata Table
cursor.execute('''
CREATE TABLE IF NOT EXISTS feature_metadata (
    feature_name TEXT PRIMARY KEY,
    description TEXT,
    source TEXT,
    version TEXT
);
''')

# Create Feature Data Table with dynamic column types
cursor.execute('''
CREATE TABLE IF NOT EXISTS feature_data (
    customer_id TEXT PRIMARY KEY,
    {}
);
'''.format(", ".join([
    f'"{col}" TEXT' if col in categorical_cols else f'"{col}" REAL'
    for col in df.columns if col != "customer_id"
])))

# Create Dataset Versioning Table
cursor.execute('''
CREATE TABLE IF NOT EXISTS dataset_versions (
    version_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_name TEXT,
    version TEXT,
    timestamp TEXT
);
''')

conn.commit()

# Insert Feature Data
df.to_sql("feature_data", conn, if_exists="replace", index=False)

# Insert Metadata (Example Entries)
metadata_entries = [
    ("SeniorCitizen", "Indicates if the customer is a senior citizen", "raw", "v1"),
    ("tenure", "Number of months the customer has stayed", "raw", "v1"),
    ("MonthlyCharges", "Monthly cost of the subscription", "raw", "v1"),
]
cursor.executemany("INSERT OR IGNORE INTO feature_metadata VALUES (?, ?, ?, ?)", metadata_entries)

# Insert Dataset Version Entry
cursor.execute("""
INSERT INTO dataset_versions (dataset_name, version, timestamp)
VALUES (?, ?, ?)
""", ("transformed_data", "v1", datetime.datetime.now().isoformat()))

conn.commit()
conn.close()

print("✅ Feature store initialized with version tracking.")
