"""
Flight Pricing Pipeline DAG
Runs daily: Ingest → dbt run → dbt test
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='flight_pricing_pipeline',
    default_args=default_args,
    description='Daily flight pricing data pipeline',
    schedule_interval='@daily',  
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['flight-pricing', 'dbt', 'snowflake'],
) as dag:

    # Task 1: Ingest new flight data to Snowflake RAW
    ingest_data = BashOperator(
        task_id='ingest_flight_data',
        bash_command='source /opt/airflow/.env 2>/dev/null || true && cd /opt/airflow/pipeline && python ingest.py --records 1000',
    )

    # Task 2: Run dbt models (staging → analytics)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt && dbt run',
    )

    # Task 3: Run dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt && dbt test',
    )

    # Define task dependencies
    ingest_data >> dbt_run >> dbt_test
