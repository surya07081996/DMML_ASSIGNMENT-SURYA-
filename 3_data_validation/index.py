import pandas as pd

def data_validation():
    df = pd.read_csv("1_data_ingestion/data/Bank Customer Churn Prediction.csv")
    df_numeric = df.select_dtypes(include=['number'])
    df_categorical = df.select_dtypes(include=['object'])

    # 1️ Check for missing values
    missing_values = df.isnull().sum()

    # 2️ Check for duplicate records (based on customer_id)
    duplicates = df.duplicated(subset=['customer_id']).sum()

    # 3️ Validate data types
    data_types = df.dtypes

    # 4️ Detect anomalies (outliers using IQR)
    Q1 = df_numeric.quantile(0.25)
    Q3 = df_numeric.quantile(0.75)
    IQR = Q3 - Q1
    outliers = ((df_numeric < (Q1 - 1.5 * IQR)) | (df_numeric > (Q3 + 1.5 * IQR))).sum()

    # 5️ Identify anomalies (negative values where not expected)
    anomalies = df_numeric[df_numeric < 0].sum()

    # 6️ Include categorical column details
    unique_values = df_categorical.nunique()  # Count unique values per column
    most_frequent = df_categorical.mode().iloc[0]  # Most frequent values

    # 🛠 Fix: Convert each part into DataFrame & Concatenate
    report = pd.concat([
        pd.DataFrame({"Missing Values": missing_values}),
        pd.DataFrame({"Duplicates": [duplicates] + [None] * (len(missing_values) - 1)}),
        pd.DataFrame({"Data Type": data_types}),
        pd.DataFrame({"Outliers": outliers}),
        pd.DataFrame({"Anomalies": anomalies}),
        pd.DataFrame({"Unique Values": unique_values}),
        pd.DataFrame({"Most Frequent": most_frequent})
    ], axis=1)

    # Save data quality report
    report.to_csv("3_data_validation/data_quality_report.csv", index=True)
    print(" Data quality report saved as 'data_quality_report.csv'")

data_validation()