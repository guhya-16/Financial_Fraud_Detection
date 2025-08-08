import asyncio
import json
import datetime
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from pipelines.preprocess_and_train import predict_transaction

# First create the FastAPI app instance
app = FastAPI()

# Then add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Now you can use the @app.websocket decorator
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("WebSocket connection established")
    
    try:
        consumer = KafkaConsumer(
            'transactions',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5000  # 5 second timeout
        )
        print("Kafka consumer created successfully")

        for message in consumer:
            try:
                txn = message.value
                result = predict_transaction(txn)

                output = {
                    "type": "transaction",
                    "data": {
                        "transaction": txn,
                        "probability": round(result["fraud_probability"], 3),
                        "prediction": result["prediction"],
                        "timestamp": datetime.datetime.now().isoformat()
                    }
                }

                await websocket.send_text(json.dumps(output))
            except Exception as e:
                print(f"Error processing message: {e}")
                continue

    except Exception as e:
        print(f"Kafka connection failed: {e}")
        # Send error message to frontend
        await websocket.send_text(json.dumps({
            "type": "error",
            "message": "Kafka connection failed. Please ensure Kafka is running.",
            "details": str(e)
        }))
        
        # Send a test message to show the frontend is working
        await websocket.send_text(json.dumps({
            "type": "test",
            "message": "WebSocket connection is working, but Kafka is not available"
        }))
        
    finally:
        try:
            consumer.close()
        except:
            pass
        await websocket.close()