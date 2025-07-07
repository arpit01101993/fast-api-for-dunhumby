from fastapi import FastAPI, HTTPException
import pandas as pd
#from .models import Event
#from .kafka_producer import push_to_kafka
#from .config import KAFKA_TOPIC

app = FastAPI(title="FastAPI Kafka Ingestor",
    description="This API ingests events and pushes them to Kafka.",
    version="2.0",
    docs_url="/swagger",  # custom URL
    redoc_url="/redoc",   # custom Redoc URL
    openapi_url="/openapi.json")
# Load CSV once at startup (adjust path!)
df = pd.read_csv("./data/raw/add_to_cart.csv")

@app.get("/")
def read_root():
    return {"message": "FastAPI Kafka Ingestor running"}

@app.get("/get-user/{user_id}")
def get_user_data(user_id: str):
    # Filter DataFrame
    user_data = df[df['user_id'] == user_id]

    if user_data.empty:
        raise HTTPException(status_code=404, detail="User ID not found")
    
    # Convert to JSON
    result = user_data.to_dict(orient="records")
    return {"user_id": user_id, "records": result}

#event: Event
@app.get("/ingest")
async def ingest_event():
    try:
        #push_to_kafka(KAFKA_TOPIC, event.json())
        push_to_kafka(KAFKA_TOPIC, df.head().to_json(orient="records")) #for now mocking records
        return {"status": "success", "event":df.head().to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
