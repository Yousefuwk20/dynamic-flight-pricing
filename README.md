# Dynamic Flight Pricing Pipeline

An end-to-end Data Engineering and ML solution for airline price prediction built on the Modern Data Stack.

## Project Structure

```
Dynamic pricing pipeline/
│
├── api/                        # FastAPI prediction service
│   ├── app.py                  # Main API application
│   ├── pricing_system.py       # Dynamic pricing logic
│   ├── flight_pricing_model.json
│   ├── label_encoders.pkl
│   └── requirements.txt
│
├── config/                     # Configuration files
│   └── snowflake_config.sql    # Snowflake setup (DB, warehouse, S3 integration)
│
├── dbt/                        # Data transformations
│   ├── models/                 # SQL transformation models
│   │   ├── staging/            # Silver layer
│   │   ├── analytics/          # Gold layer (star schema)
│   ├── tests/                  # Data quality tests
│   └── dbt_project.yml
│
├── deployment/                 # Infrastructure & deployment
│   ├── aws/                    # AWS config files
│   │   ├── task-definition.json
│   │   ├── service-definition.json
│   │   └── trust-policy.json
│   ├── scripts/                # Deployment scripts
│   │   ├── deploy.ps1
│   │   └── rollback.ps1
│   ├── dockerfile
│   └── DEPLOYMENT.md
│
├── docs/                       # Documentation
│   ├── images/                 # Visualization charts (12 PNGs)
│   ├── documentation.html      # Full HTML documentation
│   ├── documentation.md        # Markdown documentation
│   └── doc.pdf                 # PDF version
│
├── frontend/                   # Web UI
│   └── index.html              # Prediction interface
│
├── full_pipeline_demo/         # End-to-end demo pipeline
│   ├── config.py               # Central configuration
│   ├── data/                   # Generated demo data
│   ├── models/                 # Trained demo models
│   ├── output/                 # Prediction outputs
│   ├── demo_data_generator.py  # Generate synthetic flights
│   ├── demo_ingest.py          # Upload to Snowflake
│   ├── demo_transform.py       # SQL transformations
│   ├── demo_train.py           # XGBoost training + MLflow
│   ├── demo_predict.py         # Batch predictions
│   ├── run_full_demo.py        # Run complete pipeline
│   └── README.md
│
├── model/                      # Production model artifacts
│   ├── flight_pricing_model.json
│   └── label_encoders.pkl
│
├── notebooks/                  # Jupyter notebooks
│   └── xgboost.ipynb           # Model training notebook
│
├── pipeline/                   # Orchestration
│   ├── dagster_pipeline/       # Dagster assets & definitions
│   ├── snowflake_connector.py  # Database connector
│   ├── .env                    # Environment variables
│   └── requirements.txt
│
├── scripts/                    # Utility scripts
│   └── convert_to_parquet.py   # CSV to Parquet converter
│
└── README.md                   # This file
```

## Key Components

| Component | Technology | Purpose |
|-----------|------------|---------|
| Data Warehouse | Snowflake | Storage & transformations |
| ELT | dbt | SQL-based data modeling |
| ML | XGBoost | Price prediction |
| API | FastAPI | Real-time inference |
| Deployment | AWS ECS Fargate | Serverless containers |
| Orchestration | Dagster | Pipeline scheduling |

## Data Lineage (dbt)

![dbt Lineage Graph](docs/images/dbt_lineage.png)

## Model Performance

| Metric | Value |
|--------|-------|
| R² Score | 91% |
| RMSE | $51.93 |
| MAE | $33.97 |

## Documentation

See `docs/documentation.html` for complete technical documentation.
