from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class SpotRate(BaseModel):
    price: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None

class SpotRecord(BaseModel):
    time: datetime
    metal: str
    currency: str
    unit: str = "toz"
    rate: SpotRate
