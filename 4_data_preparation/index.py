import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, LabelEncoder
import seaborn as sns

def data_preparation():
    df = pd.read_csv("1_data_ingestion/data/Bank Customer Churn Prediction.csv")
    
    # Handle missing values
    df_numeric = df.select_dtypes(include=[np.number])
    df.loc[:, df_numeric.columns] = df_numeric.fillna(df_numeric.mean())

    df_categorical = df.select_dtypes(include=['object'])
    for col in df_categorical.columns:
        df.loc[:, col] = df[col].fillna(df[col].mode()[0])

    # Encode categorical variables
    le = LabelEncoder()
    df['gender'] = le.fit_transform(df['gender'])
    df['country'] = le.fit_transform(df['country'])

    # Normalize numerical columns
    scaler = MinMaxScaler()
    numeric_cols = ['credit_score', 'age', 'tenure', 'balance', 'estimated_salary']
    df.loc[:, numeric_cols] = scaler.fit_transform(df[numeric_cols])

    # Exploratory Data Analysis (EDA) - Save plots instead of showing them
    plt.figure(figsize=(10, 5))
    sns.histplot(df['age'], bins=20, kde=True)
    plt.title('Age Distribution')
    plt.savefig("4_data_preparation/age_distribution.png")
    plt.close()

    plt.figure(figsize=(10, 5))
    sns.boxplot(x=df['churn'], y=df['balance'])
    plt.title('Balance vs Churn')
    plt.savefig("4_data_preparation/balance_vs_churn.png")
    plt.close()

    # Save cleaned dataset
    df.to_csv("4_data_preparation/cleaned_data.csv", index=False)
    print("Cleaned data saved as 'cleaned_data.csv'")

data_preparation()
