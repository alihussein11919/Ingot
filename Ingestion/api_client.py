import requests
from .config import BASE_URL, METALS_API_KEY

def get(endpoint, params=None):
    params = params or {}
    params["api_key"] = METALS_API_KEY
    response = requests.get(
        BASE_URL + endpoint,
        params=params,
        timeout=30
    )
    response.raise_for_status()
    return response.json()