from ..api_client import get

def authority():
    return get("/v1/metal/authority", params={"authority": "lbma", "currency": "USD"})