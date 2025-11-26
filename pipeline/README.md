# Flight Pricing ML Pipeline

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         AIRFLOW ORCHESTRATION (MWAA / Local)                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐     │
│  │   DAILY      │   │  EVERY 6H    │   │   WEEKLY     │   │   ON-DEMAND  │     │
│  │ Data Pipeline│   │ Batch Scoring│   │ Model Train  │   │   Triggers   │     │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘     │
│         │                  │                  │                  │              │
│         ▼                  ▼                  ▼                  ▼              │
│  ┌──────────────────────────────────────────────────────────────────────┐      │
│  │                           SNOWFLAKE                                   │      │
│  │   RAW → STAGING → ANALYTICS → ML_PREDICTIONS                         │      │
│  │   (S3)   (dbt)     (dbt)        (Python)                             │      │
│  └──────────────────────────────────────────────────────────────────────┘      │
│                                    │                                            │
│                                    ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐      │
│  │                         AWS ECS (Fargate)                             │      │
│  │                    Flight Pricing API (FastAPI)                       │      │
│  │                   + XGBoost Model + Dynamic Pricing                   │      │
│  └──────────────────────────────────────────────────────────────────────┘      │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Pipeline Components

### 1. Data Pipeline (Daily - 6:00 AM)
- **DAG**: `dag_data_pipeline.py`
- **Schedule**: `0 6 * * *`
- **Tasks**:
  1. Check S3 for new data
  2. Load to Snowflake RAW schema
  3. Run dbt staging models
  4. Run dbt dimension models
  5. Run dbt fact models
  6. Run dbt tests
  7. Update data quality metrics

### 2. Batch Scoring (Every 6 Hours)
- **DAG**: `dag_batch_scoring.py`
- **Schedule**: `0 */6 * * *`
- **Tasks**:
  1. Health check on ML API
  2. Refresh ML features (dbt)
  3. Score flights (call API)
  4. Update pricing tables
  5. Send notifications

### 3. Model Training (Weekly - Sunday 2:00 AM)
- **DAG**: `dag_model_training.py`
- **Schedule**: `0 2 * * 0`
- **Tasks**:
  1. Refresh all dbt models
  2. Check data quality (branch)
  3. Train new XGBoost model
  4. Compare with current model (branch)
  5. Deploy to ECS (if better)
  6. Run smoke tests
  7. Log training results

## Setup Instructions

### 1. Snowflake Setup
```sql
-- Run the setup script
@snowflake_setup.sql
```

### 2. AWS Setup
```bash
# Create S3 bucket for models
aws s3 mb s3://flight-pricing-ml

# Create folders
aws s3api put-object --bucket flight-pricing-ml --key models/
aws s3api put-object --bucket flight-pricing-ml --key raw-data/
```

### 3. Airflow Setup (Local)
```bash
# Install Airflow with providers
pip install apache-airflow
pip install apache-airflow-providers-snowflake
pip install apache-airflow-providers-amazon

# Copy DAGs
cp pipeline/airflow_dags/*.py ~/airflow/dags/

# Copy pipeline modules
cp -r pipeline/ ~/airflow/dags/pipeline/

# Start Airflow
airflow standalone
```

### 4. Airflow Setup (AWS MWAA)
```bash
# Upload DAGs to S3
aws s3 sync pipeline/airflow_dags/ s3://your-mwaa-bucket/dags/

# Upload requirements
echo "snowflake-connector-python
xgboost
pandas
numpy
scikit-learn
boto3
requests" > requirements.txt
aws s3 cp requirements.txt s3://your-mwaa-bucket/requirements.txt
```

### 5. Configure Connections

**Snowflake Connection** (in Airflow UI):
```
Conn ID: snowflake_default
Conn Type: Snowflake
Host: your_account.snowflakecomputing.com
Schema: ANALYTICS
Login: your_user
Password: your_password
Extra: {"warehouse": "COMPUTE_WH", "database": "FLIGHT_PRICING", "role": "DATA_ENGINEER"}
```

**AWS Connection**:
```
Conn ID: aws_default
Conn Type: Amazon Web Services
Login: AWS_ACCESS_KEY_ID
Password: AWS_SECRET_ACCESS_KEY
Extra: {"region_name": "us-east-1"}
```

## Environment Variables

```bash
# .env file
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=FLIGHT_PRICING

AWS_REGION=us-east-1
S3_BUCKET=flight-pricing-ml
ML_API_ENDPOINT=http://flight-pricing-alb-1323541905.us-east-1.elb.amazonaws.com
```

## Monitoring

### Airflow UI
- Monitor DAG runs
- View task logs
- Trigger manual runs

### Snowflake Tables
```sql
-- Check latest scoring run
SELECT * FROM ML_PREDICTIONS.SCORING_LOG ORDER BY RUN_TIMESTAMP DESC LIMIT 10;

-- Check model versions
SELECT * FROM ML_PREDICTIONS.MODEL_REGISTRY ORDER BY DEPLOYED_AT DESC;

-- Check training history
SELECT * FROM ML_PREDICTIONS.TRAINING_LOG ORDER BY TRAINING_DATE DESC;

-- Check data quality
SELECT * FROM ML_PREDICTIONS.DATA_QUALITY_LOG ORDER BY CHECK_DATE DESC LIMIT 30;
```

### API Health
```bash
curl http://flight-pricing-alb-1323541905.us-east-1.elb.amazonaws.com/health
```

## Troubleshooting

### Model OOM Error
- Increase ECS task memory (currently 2GB)
- Check task-definition.json

### API Connection Error
- Verify security group allows inbound traffic
- Check ALB health checks
- Review ECS service logs

### Snowflake Connection Error
- Verify credentials in Airflow connections
- Check warehouse is running
- Verify role permissions

## Future Improvements

1. **Real-time Pricing**: Add Kafka/Kinesis for real-time price updates
2. **A/B Testing**: Deploy champion/challenger models
3. **Feature Store**: Implement Feast or Tecton for feature management
4. **MLflow**: Add experiment tracking
5. **Great Expectations**: Add data validation
