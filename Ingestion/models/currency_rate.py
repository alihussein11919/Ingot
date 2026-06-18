from sqlalchemy import Column, String, Float, DateTime
from .base import Base

class CurrencyRate(Base):
    __tablename__ = "currency_rates"

    time     = Column(DateTime, primary_key=True)
    currency = Column(String, primary_key=True)
    rate     = Column(Float)
