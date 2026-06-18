from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/ingestion')

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

def run_latest():
    from Ingestion.collectors.latest import latest
    from Ingestion.producer import publish
    publish("latest_prices", latest())

def run_spot():
    from Ingestion.collectors.spot import spot
    from Ingestion.producer import publish
    data = spot()
    for record in data:
        publish("spot_prices", record)

def run_currencies():
    from Ingestion.collectors.currencies import currencies
    from Ingestion.producer import publish
    publish("currency_rates", currencies())

def run_authority():
    from Ingestion.collectors.authority import authority
    from Ingestion.producer import publish
    publish("authority_prices", authority())

with DAG(
    dag_id='metals_ingestion_pipeline',
    default_args=default_args,
    description='Collect metal prices and publish to Kafka every 10 minutes',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['metals', 'kafka', 'ingestion'],
) as dag:

    t1 = PythonOperator(
        task_id='collect_latest_prices',
        python_callable=run_latest,
    )

    t2 = PythonOperator(
        task_id='collect_spot_prices',
        python_callable=run_spot,
    )

    t3 = PythonOperator(
        task_id='collect_currency_rates',
        python_callable=run_currencies,
    )

    t4 = PythonOperator(
        task_id='collect_authority_prices',
        python_callable=run_authority,
    )

    # All collectors run in parallel
    [t1, t2, t3, t4]
