from ..schemas.spot import SpotRecord, SpotRate
from ..schemas.currency import CurrencyRecord
from ..schemas.authority import AuthorityRecord
from datetime import datetime, timezone
from copy import deepcopy

TROY_OZ_TO_GRAM = 31.1035

# Symbols that are metals, not currencies — filter these out from currency records
METAL_SYMBOLS = {"XAU", "XAG", "XPT", "XPD", "XBT"}

def transform_spot(record: SpotRecord) -> list[SpotRecord]:
    """Return two records: one in toz, one in grams."""
    gram_record = deepcopy(record)
    gram_record.unit = "g"
    gram_record.rate.price = round(record.rate.price / TROY_OZ_TO_GRAM, 6)
    if gram_record.rate.bid:
        gram_record.rate.bid = round(record.rate.bid / TROY_OZ_TO_GRAM, 6)
    if gram_record.rate.ask:
        gram_record.rate.ask = round(record.rate.ask / TROY_OZ_TO_GRAM, 6)
    if gram_record.rate.high:
        gram_record.rate.high = round(record.rate.high / TROY_OZ_TO_GRAM, 6)
    if gram_record.rate.low:
        gram_record.rate.low = round(record.rate.low / TROY_OZ_TO_GRAM, 6)

    toz_record = deepcopy(record)
    toz_record.unit = "toz"

    return [toz_record, gram_record]


def transform_currencies(records: list[CurrencyRecord]) -> list[CurrencyRecord]:
    """Filter out metal symbols from currency records."""
    return [r for r in records if r.currency not in METAL_SYMBOLS]


def transform_authority(records: list[AuthorityRecord]) -> list[AuthorityRecord]:
    """
    Parse composite metal names like 'lbma_gold_am' into:
    - authority: lbma
    - metal: gold
    - session: am/pm
    Returns both toz and gram records.
    """
    result = []
    for r in records:
        parts = r.metal.split("_")
        metal = parts[1] if len(parts) >= 2 else r.metal
        session = parts[2] if len(parts) >= 3 else "default"

        toz = AuthorityRecord(
            time=r.time,
            authority=r.authority,
            metal=metal,
            session=session,
            unit="toz",
            price=round(r.price, 6),
            currency=r.currency
        )
        gram = AuthorityRecord(
            time=r.time,
            authority=r.authority,
            metal=metal,
            session=session,
            unit="g",
            price=round(r.price / TROY_OZ_TO_GRAM, 6),
            currency=r.currency
        )
        result.extend([toz, gram])
    return result
