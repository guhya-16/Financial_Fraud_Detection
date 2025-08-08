import streamlit as st
import pandas as pd
import joblib
import time
from pipelines.preprocess_and_train import engineer_features

# Load models and preprocessors
model = joblib.load("xgb_fraud_model.pkl")
scaler = joblib.load("scaler.pkl")
le_type = joblib.load("type_encoder.pkl")
features = joblib.load("feature_columns.pkl")

# Load a CSV stream of transactions
DATA_PATH = "data/raw_transactions.csv"
df = pd.read_csv(DATA_PATH)

# Sample 10 rows to simulate a stream (or use entire set)
stream_data = df.sample(10, random_state=int(time.time()) % 10000)  # Dynamic sample

# Encode 'type'
stream_data["type_encoded"] = le_type.transform(stream_data["type"])

# Feature engineering
stream_data = engineer_features(stream_data)
X_stream = stream_data[features]
X_scaled = scaler.transform(X_stream)

# Predict
probs = model.predict_proba(X_scaled)[:, 1]
preds = model.predict(X_scaled)

# Display live results
st.set_page_config(page_title="Real-Time Fraud Detection", page_icon="📡")
st.title("📡 Real-Time AI Fraud Detection")
st.markdown("Streaming transactions and detecting **fraud** automatically.")

# Table view
results_df = stream_data.copy()
results_df["Fraud Probability"] = probs
results_df["Prediction"] = ["FRAUD 🚨" if p == 1 else "LEGIT ✅" for p in preds]

st.dataframe(results_df[["step", "amount", "type", "Fraud Probability", "Prediction"]])

# Auto-refresh every 10 seconds
st.markdown("⏱️ Auto-refreshes every **10 seconds**.")
st.experimental_rerun()
