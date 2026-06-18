from ..api_client import get


def currencies():

    return get("/v1/currencies")