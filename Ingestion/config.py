from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv(Path(__file__).resolve().parent / ".env")

METALS_API_KEY = os.getenv("METALS_API_KEY")
BASE_URL = os.getenv("BASE_URL", "https://api.metals.dev").rstrip("/")
HEADERS = {"x-api-key": METALS_API_KEY} if METALS_API_KEY else {}
DB_URL = os.getenv("DB_URL")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
