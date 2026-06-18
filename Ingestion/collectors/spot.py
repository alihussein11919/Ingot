from ..api_client import get

METALS = ["gold", "silver", "platinum", "palladium"]

def spot():
    return [get("/v1/metal/spot", params={"metal": m, "currency": "USD"}) for m in METALS]