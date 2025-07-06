from fastapi import FastAPI, HTTPException
from .models import Event
from .kafka_producer import push_to_kafka
from .config import KAFKA_TOPIC

app = FastAPI(title="FastAPI Kafka Ingestor")

@app.get("/")
def read_root():
    return {"message": "FastAPI Kafka Ingestor running"}

@app.post("/ingest")
async def ingest_event(event: Event):
    try:
        push_to_kafka(KAFKA_TOPIC, event.json())
        return {"status": "success", "event": event.dict()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
