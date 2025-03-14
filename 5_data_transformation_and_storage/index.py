import pandas as pd

def data_transformation_and_storage():
    df = pd.read_csv("1_data_ingestion/data/Bank Customer Churn Prediction.csv")
    # Feature Engineering
    df['balance_salary_ratio'] = df['balance'] / (df['estimated_salary'] + 1)  # Avoid division by zero
    df['credit_age_ratio'] = df['credit_score'] / (df['age'] + 1)

    # Save transformed data
    df.to_csv("1_data_ingestion/data/transformed_data.csv", index=False)
    print("Transformed data saved as 'transformed_data.csv'")

    # SQL Schema for Storing Processed Data
    sql_schema = """
    CREATE TABLE CustomerChurn (
        customer_id INT PRIMARY KEY,
        credit_score FLOAT,
        country VARCHAR(50),
        gender VARCHAR(10),
        age INT,
        tenure INT,
        balance FLOAT,
        products_number INT,
        credit_card INT,
        active_member INT,
        estimated_salary FLOAT,
        churn INT,
        balance_salary_ratio FLOAT,
        credit_age_ratio FLOAT
    );
    """
    with open("5_data_transformation_and_storage/database_schema.sql", "w") as f:
        f.write(sql_schema)
    print("SQL Schema saved as 'database_schema.sql'")

data_transformation_and_storage()