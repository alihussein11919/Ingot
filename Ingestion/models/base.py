from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from ..config import DB_URL

engine = create_engine(DB_URL)
Base = declarative_base()
