from sqlalchemy import Column, String, Float, DateTime
from .base import Base

class AuthorityPrice(Base):
    __tablename__ = "authority_prices"

    time      = Column(DateTime, primary_key=True)
    authority = Column(String, primary_key=True)
    metal     = Column(String, primary_key=True)
    session   = Column(String, primary_key=True)
    unit      = Column(String, primary_key=True)
    price     = Column(Float)
    currency  = Column(String)
