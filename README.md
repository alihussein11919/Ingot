# Ingot — Real-Time Precious Metals Data Pipeline

A real-time and historical data pipeline that collects live precious metal, currency, and benchmark prices, streams them through Kafka, transforms them with Pydantic, stores them in TimescaleDB, and visualizes them through Trino + Grafana. Includes 58 years of historical LBMA benchmark data. Orchestrated with Apache Airflow. 58 years of gold history (1968–2026) and 36 years of palladium history (1990–2026) and nearly 80,000 rows

## Architecture

```
metals.dev API (live)              LBMA JSON feed (historical, 1968–present)
        ↓                                       ↓
   Collectors                          One-time backfill script
        ↓                                       ↓
   Kafka Topics                                 │
        ↓                                       │
Consumer → Normalize (Pydantic) → Transform     │
   (unit conversion: troy oz → grams)           │
        ↓                                       ↓
              TimescaleDB (authority_prices)
                       ↓
                     Trino
                       ↓
                    Grafana

Orchestrated end-to-end by Apache Airflow (scheduled runs)
```

## Tech Stack

- **Python 3.11** — core language
- **Kafka** — message streaming between collection and storage
- **TimescaleDB** (PostgreSQL extension) — time-series and historical data storage
- **Pydantic** — data validation and normalization
- **SQLAlchemy** — database ORM
- **Apache Airflow** — pipeline scheduling and orchestration
- **Trino** — distributed SQL query engine
- **Grafana** — dashboards and visualization
- **Docker / Docker Compose** — containerized infrastructure

## Data Sources

| Source | Data | Depth |
|---|---|---|
| metals.dev API | Live spot prices, currency rates, authority benchmark prices | Since pipeline start, updated every Airflow run |
| LBMA JSON feed | Official AM/PM gold, platinum, palladium fixes; daily silver fix | 1968–present (gold), 1990–present (platinum/palladium), one-time backfill |

Both sources converge into the same `authority_prices` table with an identical schema, allowing continuous queries across live and historical eras.

All prices are stored in **grams** (converted from the source troy ounce figures).

## Project Structure

```
Ingestion/
├── main.py                  # Entry point — runs all live collectors
├── consumer.py               # Kafka consumer — writes to TimescaleDB
├── producer.py                 # Kafka producer
├── api_client.py                # HTTP client for metals.dev API
├── config.py                     # Environment config loader
├── db_init.py                      # Creates tables and TimescaleDB hypertables
├── lbma_backfill.py                  # One-time historical backfill from LBMA
├── collectors/                         # One file per metals.dev endpoint
│   ├── latest.py
│   ├── spot.py
│   ├── currencies.py
│   ├── authority.py
│   └── historical.py             # Scaffolded, not yet implemented
├── schemas/                          # Pydantic models for validation
├── services/
│   ├── normalize.py                    # Unpacks raw API JSON into structured records
│   └── transform.py                      # Unit conversion (toz → g), filtering, session parsing
├── models/                                 # SQLAlchemy database models
└── utils/                                    # Logging, retry, datetime helpers (scaffolded)

airflow/
└── dags/
    └── metals_pipeline.py            # Scheduled DAG — runs live collectors periodically

trino/
└── catalog/
    └── timescaledb.properties.example   # Template — copy to timescaledb.properties with real credentials

docker-compose.yml      # Airflow, Postgres (Airflow metadata), Trino, Grafana
Dockerfile.airflow       # Custom Airflow image with project dependencies installed
.env.example              # Template for required environment variables
```

## Database Schema

This follows a star schema pattern, with dimensions (metal, currency, authority, unit) denormalized directly into the fact tables — appropriate at this data volume.

### `spot_prices` (fact table — live)
Live spot prices per metal, in grams.

| Column | Type | Description |
|---|---|---|
| `time` | timestamp | UTC collection time |
| `metal` | string | `gold`, `silver`, `platinum`, `palladium` |
| `currency` | string | Default `USD` |
| `unit` | string | `g` |
| `price` | float | Current spot price |
| `bid` / `ask` | float | Buy/sell prices |
| `high` / `low` | float | Daily range |
| `change` / `change_pct` | float | Change vs previous market day |

**Primary key:** `(time, metal, currency, unit)`

### `currency_rates` (fact table — live)
Fiat currency exchange rates relative to USD. Metal ticker symbols (XAU, XAG, etc.) are filtered out.

| Column | Type | Description |
|---|---|---|
| `time` | timestamp | UTC collection time |
| `currency` | string | ISO currency code |
| `rate` | float | Rate relative to USD |

**Primary key:** `(time, currency)`

### `authority_prices` (fact table — live + historical)
Official benchmark prices from market authorities. Currently populated from LBMA, both via metals.dev (live) and the LBMA JSON feed (historical backfill).

| Column | Type | Description |
|---|---|---|
| `time` | timestamp | UTC date/time |
| `authority` | string | `lbma` (schema supports `lme`, `mcx`, `ibja`) |
| `metal` | string | `gold`, `silver`, `platinum`, `palladium` |
| `session` | string | `am`, `pm`, or `default` (silver has no AM/PM split) |
| `unit` | string | `g` |
| `price` | float | Benchmark price |
| `currency` | string | Default `USD` |

**Primary key:** `(time, authority, metal, session, unit)`

**Current historical depth:**
- Gold: 1968–present (AM and PM fixes)
- Silver: 1968–present (single daily fix)
- Platinum: 1990–present (AM and PM fixes)
- Palladium: 1990–present (AM and PM fixes)

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

### 5. Run the one-time historical backfill
```bash
python -m Ingestion.lbma_backfill
```
This fetches and loads 58 years of LBMA benchmark data. Safe to re-run — uses upsert, so it won't create duplicates.

### 6. Start Airflow, Trino, and Grafana
```bash
docker compose up -d
```

### 7. Run the live pipeline manually (without Airflow)
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

Three dashboards, each telling a different part of the story:

**The Long Arc** — 58 years of gold history in one continuous line, decade-by-decade volatility, and the gold/silver ratio across its full historical range.

**Metal Personalities** — comparative performance of gold, silver, platinum, and palladium normalized to a common starting point, plus rolling volatility per metal to show which behaves most erratically.

**Today in Context** — live spot price compared against all-time high/low and the 10-year average, plus a live-vs-LBMA-fix divergence tracker and pipeline health monitoring.

## Known Limitations

- **API quota:** the free metals.dev plan allows 100 requests/month. Each pipeline run uses ~7 requests — schedule the Airflow DAG accordingly.
- **LBMA data licensing:** the JSON feed used for backfill is intended for individual/personal use (as used by tools like Portfolio Performance). Official licensed historical data requires an IBA or LME license for commercial use — see lbma.org.uk for details.
- **`historical.py` (metals.dev collector), `utils/`:** scaffolded but not yet implemented; superseded by the LBMA backfill for historical needs.

## Roadmap

- [ ] Grafana alerting rules (price spikes, stale data, pipeline failures)
- [ ] CI/CD for automated testing
- [ ] Production-grade secrets management
- [ ] Periodic re-sync of LBMA data (currently a one-time backfill; could add a small monthly job to catch any LBMA corrections/updates)
