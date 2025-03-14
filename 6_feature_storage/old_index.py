import pandas as pd
from feast import Entity, FeatureView, FileSource, Field, FeatureStore, ValueType
from feast.types import Float32, Int64

# ✅ Step 1: Load the Parquet File
file_path = r"1_data_ingestion/data/transformed_data.csv"

df = pd.read_csv(file_path)

# ✅ Step 2: Ensure Timestamp Column Exists
timestamp_column = "event_timestamp"

if timestamp_column not in df.columns:
    print(f"❌ Column '{timestamp_column}' not found. Adding it now...")
    df[timestamp_column] = pd.Timestamp.now()
    df.to_csv(file_path, index=False)
    print("✅ 'event_timestamp' column added and file updated.")
else:
    print("✅ Timestamp column already exists.")

print("Updated Dataset Columns:", df.columns)

# ✅ Step 3: Define Feature Store Repository Path
repo_path = r"6_feature_storage"  # Adjust path as per your project structure
feature_store = FeatureStore(repo_path)

# ✅ Step 4: Define Entity (Fixing Incorrect Placement)
customer_entity = Entity(
    name="customer_id",  
    join_keys=["customerid"],  # Ensure this matches the dataset column name
    value_type=ValueType.INT64,  
    description="A unique identifier for customers"
)

# ✅ Step 5: Define Data Source
customer_data = FileSource(
    path=file_path,
    timestamp_field="event_timestamp",
)

# ✅ Step 6: Define Feature View
customer_feature_view = FeatureView(
    name="customer_features",
    entities=[customer_entity],  # ✅ Fixed: Use the correct entity name
    schema=[
        Field(name="credit_age_ratio", dtype=Float32),
        Field(name="purchase_frequency", dtype=Float32),
    ],
    source=customer_data
)

print("✅ Feature View created successfully!")

# ✅ Step 7: Apply Entity & Feature View to Feature Store
feature_store.apply([customer_entity, customer_feature_view])

# # ✅ Step 8: Retrieve Features from the Online Store
# features = feature_store.get_online_features(
#     features=[
#         "customer_features:credit_age_ratio",
#         "customer_features:purchase_frequency",
#     ],
#     entity_rows=[{"customer_id": 12345}],  # Ensure this key matches the entity name
# ).to_dict()

# print("🔍 Retrieved Features:", features)
