from ..api_client import get

def latest():

    return get("/v1/latest")