import matplotlib
matplotlib.use('Agg')  # ✅ Use a non-interactive backend

import sqlite3
import pandas as pd
import mlflow
import mlflow.sklearn
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, roc_curve
)

# 🎯 Load Data from SQLite
DB_PATH = "feature_store.db"
conn = sqlite3.connect(DB_PATH)
query = "SELECT * FROM feature_data;"
df = pd.read_sql(query, conn)
conn.close()

print("Columns in dataset:", df.columns.tolist())

# Remove customerID_* columns
df = df.loc[:, ~df.columns.str.startswith("customerID_")]

# Drop 'customer_id' if it exists
if "customer_id" in df.columns:
    df.drop(columns=["customer_id"], inplace=True)

print("Columns after removing customer IDs:", df.columns.tolist())

# 🎯 Define Features and Target
X = df.drop(columns=["churn"])  # Features
y = df["churn"]  # Target (1 = Churn, 0 = No Churn)

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Standardize numerical features
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# 🎯 Set up MLflow
mlflow.set_experiment("Customer Churn Prediction")

# 🎯 Train & Evaluate Models
def train_model(model, model_name):
    with mlflow.start_run():
        # Train
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        y_pred_prob = model.predict_proba(X_test)[:, 1]  # Get probability scores for ROC

        # Evaluate
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)
        roc_auc = roc_auc_score(y_test, y_pred_prob)

        # Log Metrics
        mlflow.log_param("model_name", model_name)
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("roc_auc", roc_auc)

        # 🎯 Plot and Save ROC Curve
        fpr, tpr, _ = roc_curve(y_test, y_pred_prob)
        plt.figure(figsize=(8, 6))
        plt.plot(fpr, tpr, label=f"{model_name} (AUC = {roc_auc:.4f})", color='blue')
        plt.plot([0, 1], [0, 1], linestyle='--', color='gray')
        plt.xlabel("False Positive Rate")
        plt.ylabel("True Positive Rate")
        plt.title("ROC Curve")
        plt.legend(loc="lower right")
        plt.grid()

        # ✅ Save instead of showing the plot
        roc_curve_path = f"{model_name}_roc_curve.png"
        plt.savefig(roc_curve_path)
        mlflow.log_artifact(roc_curve_path)
        plt.close()

        # Log Model
        mlflow.sklearn.log_model(model, model_name, input_example=X_test[:5])

        print(f"✅ {model_name} - Accuracy: {accuracy:.4f}, Precision: {precision:.4f}, Recall: {recall:.4f}, F1 Score: {f1:.4f}, ROC-AUC: {roc_auc:.4f}")

# 🎯 Train Logistic Regression
train_model(LogisticRegression(max_iter=1000), "Logistic Regression")

# 🎯 Train Random Forest
train_model(RandomForestClassifier(n_estimators=100), "Random Forest")

print("🎯 Model training complete! Check MLflow for details.")
