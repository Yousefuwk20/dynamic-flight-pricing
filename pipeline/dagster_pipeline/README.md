# Flight Pricing ML Pipeline - Dagster

This Dagster pipeline orchestrates the complete flight pricing ML workflow from data generation to predictions.

## Pipeline Overview

```
generate_demo_data → ingest_to_snowflake → transform_data → train_demo_model → batch_predictions
```

### Assets

| Asset | Description |
|-------|-------------|
| `generate_demo_data` | Generate 500 synthetic flight records |
| `ingest_to_snowflake` | Load data to Snowflake `DEMO_RAW.DEMO_FLIGHTS` |
| `transform_data` | Transform through staging → analytics → ML layers |
| `train_demo_model` | Train XGBoost model with MLflow tracking |
| `batch_predictions` | Score flights and generate prediction report |

### Snowflake Tables Created

- `DEMO_RAW.DEMO_FLIGHTS` - Raw flight data
- `DEMO_STAGING.STG_DEMO_FLIGHTS` - Cleaned/staged data
- `DEMO_ANALYTICS.DEMO_DIM_AIRLINES` - Airlines dimension
- `DEMO_ANALYTICS.DEMO_DIM_AIRPORTS` - Airports dimension
- `DEMO_ANALYTICS.DEMO_DIM_ROUTES` - Routes dimension
- `DEMO_ANALYTICS.DEMO_FACT_FLIGHTS` - Flight facts
- `DEMO_ML.DEMO_ML_FEATURES` - ML feature table

## Quick Start

### 1. Start Services

```powershell
# Start MLflow (port 5000)
cd "d:\Dynamic pricing pipeline\ml"
python -m mlflow server --host 0.0.0.0 --port 5000

# Start Dagster (port 3000)
cd "d:\Dynamic pricing pipeline\ml\pipeline\dagster_pipeline"
python -m dagster dev -p 3000
```

### 2. Run Pipeline

1. Open Dagster UI: http://localhost:3000
2. Go to **Jobs** → **full_pipeline_job**
3. Click **Launch Run**

### 3. View Results

- **Dagster**: http://localhost:3000 - Pipeline runs, asset lineage
- **MLflow**: http://localhost:5000 - Model metrics, experiments

## Schedule

| Schedule | Cron | Description |
|----------|------|-------------|
| `weekly_full_pipeline` | `0 2 * * 0` | Run full pipeline every Sunday at 2 AM |

## File Structure

```
dagster_pipeline/
├── __init__.py          # Package init (imports from definitions)
├── definitions.py       # Main Dagster definitions (assets, jobs, schedules)
├── README.md            # This file
├── requirements.txt     # Python dependencies
└── _old_pipeline_backup/ # Archived old pipeline files
```

## Environment Variables

Set these in `ml/pipeline/.env`:

```env
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=FLIGHT_PRICING
```

## Model Metrics

After training, the model logs to MLflow:
- **R² Score**: ~0.71
- **RMSE**: ~$128
- **MAE**: ~$75
- **MAPE**: ~4%
