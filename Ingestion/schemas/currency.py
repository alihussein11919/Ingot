from pydantic import BaseModel
from datetime import datetime

class CurrencyRecord(BaseModel):
    time: datetime
    currency: str
    rate: float
