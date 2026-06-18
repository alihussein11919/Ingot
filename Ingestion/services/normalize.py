from datetime import datetime, timezone
from ..schemas.spot import SpotRecord, SpotRate
from ..schemas.currency import CurrencyRecord
from ..schemas.authority import AuthorityRecord


def normalize_spot(raw: dict) -> SpotRecord:
    return SpotRecord(
        time=datetime.now(timezone.utc),
        metal=raw["metal"],
        currency=raw.get("currency", "USD"),
        rate=SpotRate(
            price=raw["rate"]["price"],
            bid=raw["rate"].get("bid"),
            ask=raw["rate"].get("ask"),
            high=raw["rate"].get("high"),
            low=raw["rate"].get("low"),
            change=raw["rate"].get("change"),
            change_percent=raw["rate"].get("change_percent"),
        )
    )


def normalize_currencies(raw: dict) -> list[CurrencyRecord]:
    now = datetime.now(timezone.utc)
    return [
        CurrencyRecord(time=now, currency=currency, rate=rate)
        for currency, rate in raw.get("currencies", {}).items()
    ]


def normalize_authority(raw: dict) -> list[AuthorityRecord]:
    now = datetime.now(timezone.utc)
    authority = raw.get("authority")
    currency = raw.get("currency", "USD")
    return [
        AuthorityRecord(time=now, authority=authority, metal=metal, price=price, currency=currency)
        for metal, price in raw.get("rates", {}).items()
    ]
