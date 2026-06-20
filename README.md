# Ingot — Real-Time Precious Metals Data Pipeline

A real-time data pipeline that collects live precious metal, currency, and benchmark prices, streams them through Kafka, transforms them with Pydantic, stores them in TimescaleDB, and visualizes them through Trino + Grafana. Orchestrated with Apache Airflow.

## Architecture

```
metals.dev API
      ↓
Collectors (latest, spot, currencies, authority)
      ↓
Kafka Topics
      ↓
Consumer → Normalize (Pydantic) → Transform (unit conversion, parsing)
      ↓
TimescaleDB
      ↓
Trino (SQL query layer)
      ↓
Grafana (dashboards)

Orchestrated end-to-end by Apache Airflow (scheduled runs)
```

## Tech Stack

- **Python 3.11** — core language
- **Kafka** — message streaming between collection and storage
- **TimescaleDB** (PostgreSQL extension) — time-series data storage
- **Pydantic** — data validation and normalization
- **SQLAlchemy** — database ORM
- **Apache Airflow** — pipeline scheduling and orchestration
- **Trino** — distributed SQL query engine
- **Grafana** — dashboards and visualization
- **Docker / Docker Compose** — containerized infrastructure

## Data Collected

| Data | Source Endpoint | Description |
|---|---|---|
| Spot Prices | `/v1/metal/spot` | Real-time gold, silver, platinum, palladium prices |
| Currency Rates | `/v1/currencies` | 170+ live currency conversion rates |
| Authority Prices | `/v1/metal/authority` | Official benchmark prices (LBMA AM/PM fixes) |
| Latest Prices | `/v1/latest` | Combined snapshot of all metals and currencies |

All metal prices are stored in **both troy ounces and grams**.

## Project Structure

```
Ingestion/
├── main.py                  # Entry point — runs all collectors
├── consumer.py               # Kafka consumer — writes to TimescaleDB
├── producer.py                 # Kafka producer
├── api_client.py                # HTTP client for metals.dev API
├── config.py                     # Environment config loader
├── db_init.py                      # Creates tables and TimescaleDB hypertables
├── collectors/                       # One file per API endpoint
│   ├── latest.py
│   ├── spot.py
│   ├── currencies.py
│   ├── authority.py
│   └── historical.py             # Scaffolded, not yet implemented
├── schemas/                          # Pydantic models for validation
├── services/
│   ├── normalize.py                    # Unpacks raw API JSON into structured records
│   └── transform.py                      # Unit conversion (toz/g), filtering, session parsing
├── models/                                 # SQLAlchemy database models
└── utils/                                    # Logging, retry, datetime helpers (scaffolded)

airflow/
└── dags/
    └── metals_pipeline.py            # Scheduled DAG — runs collectors periodically

trino/
└── catalog/
    └── timescaledb.properties.example   # Template — copy to timescaledb.properties with real credentials

docker-compose.yml      # Airflow, Postgres (Airflow metadata), Trino, Grafana
Dockerfile.airflow       # Custom Airflow image with project dependencies installed
.env.example              # Template for required environment variables
```

## Database Schema

### `spot_prices`
Live spot prices per metal, stored in both troy ounces and grams.

| Column | Type | Description |
|---|---|---|
| `time` | timestamp | UTC collection time |
| `metal` | string | `gold`, `silver`, `platinum`, `palladium` |
| `currency` | string | Default `USD` |
| `unit` | string | `toz` or `g` |
| `price` | float | Current spot price |
| `bid` / `ask` | float | Buy/sell prices |
| `high` / `low` | float | Daily range |
| `change` / `change_pct` | float | Change vs previous market day |

**Primary key:** `(time, metal, currency, unit)`

### `currency_rates`
Fiat currency exchange rates relative to USD. Metal symbols (XAU, XAG, etc.) are filtered out.

| Column | Type | Description |
|---|---|---|
| `time` | timestamp | UTC collection time |
| `currency` | string | ISO currency code |
| `rate` | float | Rate relative to USD |

**Primary key:** `(time, currency)`

### `authority_prices`
Official benchmark prices from market authorities (LBMA, LME, MCX, IBJA), stored in both units.  The London Bullion Market Association and others

| Column | Type | Description |
|---|---|---|
| `time` | timestamp | UTC collection time |
| `authority` | string | `lbma`, `lme`, `mcx`, `ibja` |
| `metal` | string | Parsed from composite field name |
| `session` | string | `am`, `pm`, or `default` |
| `unit` | string | `toz` or `g` |
| `price` | float | Benchmark price |
| `currency` | string | Default `USD` |

**Primary key:** `(time, authority, metal, session, unit)`

## Setup

### Prerequisites
- Docker Desktop
- Python 3.11
- A free [metals.dev](https://metals.dev) API key

### 1. Clone and configure environment
```bash
git clone <your-repo-url>
cd ingot2
cp .env.example .env
cp trino/catalog/timescaledb.properties.example trino/catalog/timescaledb.properties
```
Edit `.env` and `trino/catalog/timescaledb.properties` with your real API key and database credentials.

### 2. Install Python dependencies
```bash
pip install -r Ingestion/requirements.txt
```

### 3. Start core infrastructure
```bash
# Kafka
docker run -d --name kafka -p 9092:9092 apache/kafka:latest

# TimescaleDB
docker run -d --name timescaledb -p 5432:5432 \
  -e POSTGRES_USER=metals_user \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=metals_db \
  timescale/timescaledb:latest-pg15
```

### 4. Initialize the database
```bash
python -m Ingestion.db_init
```

### 5. Start Airflow, Trino, and Grafana
```bash
docker compose up -d
```

### 6. Run the pipeline manually (without Airflow)
```bash
# Terminal 1 — consumer (keep running)
python -m Ingestion.consumer

# Terminal 2 — producer (collects and publishes)
python -m Ingestion.main
```

## Accessing the Services

| Service | URL | Default Login |
|---|---|---|
| Airflow | http://localhost:8088 | admin / admin |
| Trino | http://localhost:8089 | — |
| Grafana | http://localhost:3000 | admin / admin |

## Dashboards

The Grafana dashboard includes:
- **Live price overview** — current price per metal
- **Price trends** — historical price movement, split by scale (gold/platinum vs silver/palladium)
- **Daily volatility** — price change in dollars and percent
- **Bid-ask spread** — liquidity indicator, in dollars and percent
- **Gold-to-silver ratio** — classic trading benchmark
- **LBMA AM/PM fix comparison**
- **Currency strength vs USD**
- **Pipeline health** — last update timestamp, useful for alerting on stale data

## Known Limitations

- **API quota:** the free metals.dev plan allows 100 requests/month. Each pipeline run uses ~7 requests (1 latest + 4 spot + 1 currencies + 1 authority) — schedule the Airflow DAG accordingly.
- **Historical backfill:** not currently implemented. A 5-year daily backfill would require roughly 7,300 requests, which needs a paid plan (Silver tier, $9.99/month, 10,000 requests/month is sufficient). Revisit when budget allows.
- **`historical.py`, `utils/`:** scaffolded but not yet implemented.

## Roadmap

- [ ] Historical data backfill (pending budget for paid API tier)
- [ ] Grafana alerting rules (price spikes, stale data)
- [ ] CI/CD for automated testing
- [ ] Production-grade secrets management
