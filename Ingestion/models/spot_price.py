from sqlalchemy import Column, String, Float, DateTime
from .base import Base

class SpotPrice(Base):
    __tablename__ = "spot_prices"

    time       = Column(DateTime, primary_key=True)
    metal      = Column(String, primary_key=True)
    currency   = Column(String, primary_key=True)
    unit       = Column(String, primary_key=True)
    price      = Column(Float)
    bid        = Column(Float)
    ask        = Column(Float)
    high       = Column(Float)
    low        = Column(Float)
    change     = Column(Float)
    change_pct = Column(Float)
