from pydantic import BaseModel
from datetime import datetime

class Event(BaseModel):
    user_id: str
    product_id: str
    event_type: str
    timestamp: datetime
