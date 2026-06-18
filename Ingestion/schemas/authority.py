from pydantic import BaseModel
from datetime import datetime

class AuthorityRecord(BaseModel):
    time: datetime
    authority: str
    metal: str
    session: str = "default"
    unit: str = "toz"
    price: float
    currency: str
