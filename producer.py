import pandas as pd
import json
import time
from kafka import KafkaProducer
import random

# Path to your CSV
DATA_PATH = 'data/raw_transactions.csv'

# Load full dataset
df = pd.read_csv(DATA_PATH)

# Separate fraud and non-fraud
fraud_df = df[df['isFraud'] == 1]
nonfraud_df = df[df['isFraud'] == 0]

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🚀 Starting random transaction stream (fraud + non-fraud mix)...")

try:
    while True:
        # 30% chance of picking fraud, 70% non-fraud
        if random.random() < 0.3 and len(fraud_df) > 0:
            txn = fraud_df.sample(1).to_dict(orient='records')[0]
            label = "FRAUD"
        else:
            txn = nonfraud_df.sample(1).to_dict(orient='records')[0]
            label = "LEGIT"

        # Create transaction with proper structure
        transaction = {
            'step': txn.get('step', random.randint(1, 744)),
            'amount': float(txn.get('amount', random.uniform(10, 100000))),
            'oldbalanceOrg': float(txn.get('oldbalanceOrg', random.uniform(0, 1000000))),
            'newbalanceOrig': float(txn.get('newbalanceOrig', random.uniform(0, 1000000))),
            'oldbalanceDest': float(txn.get('oldbalanceDest', random.uniform(0, 1000000))),
            'newbalanceDest': float(txn.get('newbalanceDest', random.uniform(0, 1000000))),
            'type': txn.get('type', random.choice(['TRANSFER', 'PAYMENT', 'CASH_OUT'])),
            'from_account': f"ACC{random.randint(100000, 999999)}",
            'to_account': f"ACC{random.randint(100000, 999999)}",
            'transaction_id': f"TXN{random.randint(100000, 999999)}"
        }

        # Send to Kafka topic
        producer.send('transactions', transaction)
        producer.flush()

        print(f"📤 Sent {transaction['transaction_id']} | Amount: ${transaction['amount']:.2f} | Type: {transaction['type']} | From: {transaction['from_account']} → To: {transaction['to_account']}")

        # Wait 2 seconds
        time.sleep(2)

except KeyboardInterrupt:
    print("🛑 Stopping producer...")
    producer.close()
