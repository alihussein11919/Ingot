from .models.base import Base, engine
from .models.spot_price import SpotPrice
from .models.currency_rate import CurrencyRate
from .models.authority_price import AuthorityPrice
from sqlalchemy import text

def init_db():
    Base.metadata.create_all(engine)
    with engine.connect() as conn:
        conn.execute(text("SELECT create_hypertable('spot_prices', 'time', if_not_exists => TRUE);"))
        conn.execute(text("SELECT create_hypertable('currency_rates', 'time', if_not_exists => TRUE);"))
        conn.execute(text("SELECT create_hypertable('authority_prices', 'time', if_not_exists => TRUE);"))
        conn.commit()
    print("Tables and hypertables created successfully.")

if __name__ == "__main__":
    init_db()
