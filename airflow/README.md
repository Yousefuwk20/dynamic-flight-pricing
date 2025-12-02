# Airflow Setup for Flight Pricing Pipeline

## Quick Start

### 1. Start Airflow
```bash
cd "D:\Dynamic pricing pipeline"

# Initialize the database and create admin user (first time only)
docker-compose -f docker-compose.airflow.yml up airflow-init

# Start Airflow services
docker-compose -f docker-compose.airflow.yml up -d
```

### 2. Access Airflow UI
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

### 3. Enable DAGs
1. Go to the Airflow UI
2. Find `flight_pricing_pipeline` DAG
3. Toggle it ON
4. Click "Trigger DAG" to run manually

## DAGs

### flight_pricing_pipeline
**Schedule:** Daily at midnight

| Task | Description |
|------|-------------|
| `ingest_flight_data` | Generate and load flight data to Snowflake RAW |
| `dbt_run` | Run dbt models (staging → analytics) |
| `dbt_test` | Run dbt data quality tests |

## Stop Airflow
```bash
docker-compose -f docker-compose.airflow.yml down
```