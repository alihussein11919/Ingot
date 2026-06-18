from kafka import KafkaConsumer
from sqlalchemy.orm import Session
from .models.base import engine
from .models.spot_price import SpotPrice
from .models.currency_rate import CurrencyRate
from .models.authority_price import AuthorityPrice
from .services.normalize import normalize_spot, normalize_currencies, normalize_authority
from .services.transform import transform_spot, transform_currencies, transform_authority
from .config import KAFKA_BOOTSTRAP
import json

consumer = KafkaConsumer(
    "latest_prices", "spot_prices", "currency_rates", "authority_prices",
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="metals-consumer-group"
)

def save_spot(data, session):
    raw_list = data if isinstance(data, list) else [data]
    for raw in raw_list:
        normalized = normalize_spot(raw)
        records = transform_spot(normalized)
        for r in records:
            session.merge(SpotPrice(
                time=r.time,
                metal=r.metal,
                currency=r.currency,
                unit=r.unit,
                price=r.rate.price,
                bid=r.rate.bid,
                ask=r.rate.ask,
                high=r.rate.high,
                low=r.rate.low,
                change=r.rate.change,
                change_pct=r.rate.change_percent,
            ))

def save_currencies(data, session):
    normalized = normalize_currencies(data)
    cleaned = transform_currencies(normalized)
    for r in cleaned:
        session.merge(CurrencyRate(time=r.time, currency=r.currency, rate=r.rate))

def save_authority(data, session):
    normalized = normalize_authority(data)
    transformed = transform_authority(normalized)
    for r in transformed:
        session.merge(AuthorityPrice(
            time=r.time,
            authority=r.authority,
            metal=r.metal,
            session=r.session,
            unit=r.unit,
            price=r.price,
            currency=r.currency,
        ))

def run():
    print("Consumer started, waiting for messages...")
    with Session(engine) as session:
        for msg in consumer:
            topic = msg.topic
            data = msg.value
            print(f"Received from {topic}")
            try:
                if topic == "spot_prices":
                    save_spot(data, session)
                elif topic == "currency_rates":
                    save_currencies(data, session)
                elif topic == "authority_prices":
                    save_authority(data, session)
                session.commit()
                print(f"Saved to DB: {topic}")
            except Exception as e:
                session.rollback()
                print(f"Error on {topic}: {e}")

if __name__ == "__main__":
    run()
