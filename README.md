# Ingot
<html>
<body>
<!--StartFragment--><html><head></head><body>
<hr>
<h1>Ingot — Real-Time Metals Price Ingestion Pipeline</h1>
<p>A data pipeline that collects live precious metal and currency prices, streams them through Kafka, transforms them, and stores them in TimescaleDB. Orchestrated with Airflow.</p>
<h2>What It Does</h2>
<p>This project pulls real-time data from the <a href="https://metals.dev/">metals.dev</a> API — spot prices, currency rates, and official authority benchmark prices (like LBMA) — and pushes it through a streaming pipeline into a time-series database, ready for analysis.</p>
<h2>Architecture</h2>
<pre><code>metals.dev API
      ↓
Collectors (latest, spot, currencies, authority)
      ↓
Kafka Topics
      ↓
Consumer → Normalize (Pydantic) → Transform (unit conversion, parsing)
      ↓
TimescaleDB
      ↓
Orchestrated by Airflow (scheduled runs)
</code></pre>
<h2>Tech Stack</h2>
<ul>
<li><strong>Python 3.11</strong> — core language</li>
<li><strong>Kafka</strong> — message streaming between collection and storage</li>
<li><strong>TimescaleDB</strong> (PostgreSQL extension) — time-series data storage</li>
<li><strong>Pydantic</strong> — data validation and normalization</li>
<li><strong>SQLAlchemy</strong> — database ORM</li>
<li><strong>Apache Airflow</strong> — pipeline scheduling and orchestration</li>
<li><strong>Docker</strong> — containerized infrastructure</li>
</ul>
<h2>Data Collected</h2>

Data | Source Endpoint | Description
-- | -- | --
Spot Prices | /v1/metal/spot | Real-time gold, silver, platinum, palladium prices
Currency Rates | /v1/currencies | 170+ live currency conversion rates
Authority Prices | /v1/metal/authority | Official benchmark prices (LBMA AM/PM fixes)
Latest Prices | /v1/latest | Combined snapshot of all metals and currencies


<p>All metal prices are stored in <strong>both troy ounces and grams</strong>.</p>
<h2>Project Structure</h2>
<pre><code>Ingestion/
├── main.py                  # Entry point — runs all collectors
├── consumer.py               # Kafka consumer — writes to TimescaleDB
├── producer.py                # Kafka producer
├── api_client.py              # HTTP client for metals.dev API
├── config.py                  # Environment config loader
├── db_init.py                  # Creates tables and TimescaleDB hypertables
├── collectors/                  # One file per API endpoint
│   ├── latest.py
│   ├── spot.py
│   ├── currencies.py
│   └── authority.py
├── schemas/                      # Pydantic models for validation
├── services/
│   ├── normalize.py                # Unpacks raw API JSON
│   └── transform.py                 # Unit conversion, filtering, parsing
└── models/                            # SQLAlchemy database models

airflow/
└── dags/
    └── metals_pipeline.py            # Scheduled DAG, runs every N minutes
</code></pre>
<h2>Setup</h2>
<h3>1. Install dependencies</h3>
<pre><code class="language-bash">pip install -r Ingestion/requirements.txt
</code></pre>
<h3>2. Set up environment variables</h3>
<p>Create <code>Ingestion/.env</code>:</p>
<pre><code>METALS_API_KEY=your_api_key_here
BASE_URL=https://api.metals.dev
DB_URL=postgresql://metals_user:password@127.0.0.1:5433/metals_db
KAFKA_BOOTSTRAP=localhost:9092
</code></pre>
<h3>3. Start infrastructure</h3>
<pre><code class="language-bash"># Kafka
docker run -d --name kafka -p 9092:9092 apache/kafka:latest

# TimescaleDB
docker run -d --name timescaledb -p 5433:5432 \
  -e POSTGRES_USER=metals_user \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=metals_db \
  timescale/timescaledb:latest-pg15
</code></pre>
<h3>4. Initialize the database</h3>
<pre><code class="language-bash">python -m Ingestion.db_init
</code></pre>
<h3>5. Run the pipeline manually</h3>
<p>Open two terminals:</p>
<pre><code class="language-bash"># Terminal 1 — consumer (keep running)
python -m Ingestion.consumer

# Terminal 2 — producer (collects and publishes)
python -m Ingestion.main
</code></pre>
<h3>6. Or run on a schedule with Airflow</h3>
<pre><code class="language-bash">docker compose up -d
</code></pre>
<p>Visit <code>http://localhost:8080</code> (default login: <code>admin</code> / <code>admin</code>) to view and trigger the DAG.</p>
<h2>Database Tables</h2>
<ul>
<li><strong><code>spot_prices</code></strong> — live spot prices per metal, in toz and grams</li>
<li><strong><code>currency_rates</code></strong> — fiat currency exchange rates</li>
<li><strong><code>authority_prices</code></strong> — official benchmark prices by authority and session (AM/PM)</li>
</ul>
<h2>Notes</h2>
<ul>
<li>The free metals.dev plan allows 100 requests/month. Each pipeline run uses ~7 requests, so schedule accordingly.</li>
<li>All timestamps are stored in UTC.</li>
</ul></body></html><!--EndFragment-->
</body>
