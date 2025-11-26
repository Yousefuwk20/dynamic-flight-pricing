"""
Dagster Full Pipeline Definitions
=================================
Flight Pricing ML Pipeline with Dagster orchestration and MLflow tracking.

Pipeline Steps:
1. generate_demo_data - Generate synthetic flight data
2. ingest_to_snowflake - Load data to Snowflake DEMO_RAW schema  
3. transform_data - Transform through staging, analytics, ML layers
4. train_demo_model - Train XGBoost model with MLflow tracking
5. batch_predictions - Score flights and generate prediction report
"""

from dagster import (
    Definitions,
    asset,
    AssetExecutionContext,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
    MaterializeResult,
    MetadataValue,
    Output,
    ConfigurableResource,
    AssetDep,
)

from datetime import datetime
import pandas as pd
import numpy as np
import requests
import os
import sys
from pathlib import Path

PIPELINE_DEMO_PATH = str(Path(__file__).parent.parent.parent / "full_pipeline_demo")

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))


# RESOURCES

class SnowflakeResource(ConfigurableResource):
    """Resource to connect to Snowflake"""
    account: str = os.getenv('SNOWFLAKE_ACCOUNT', '')
    user: str = os.getenv('SNOWFLAKE_USER', '')
    password: str = os.getenv('SNOWFLAKE_PASSWORD', '')
    warehouse: str = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
    database: str = os.getenv('SNOWFLAKE_DATABASE', 'FLIGHT_PRICING')
    
    def get_connection(self):
        import snowflake.connector
        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
        )
    
    def test_connection(self) -> bool:
        try:
            conn = self.get_connection()
            conn.close()
            return True
        except:
            return False


class MLflowResource(ConfigurableResource):
    """Resource for MLflow tracking"""
    tracking_uri: str = "http://localhost:5000"
    experiment_name: str = "flight-pricing-dagster"
    
    def is_available(self) -> bool:
        try:
            response = requests.get(self.tracking_uri, timeout=3)
            return response.status_code == 200
        except:
            return False


# FULL PIPELINE ASSETS

@asset(
    group_name="full_pipeline",
    description="Generate synthetic flight data for the demo",
)
def generate_demo_data(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    """Step 1: Generate synthetic flight data"""
    if PIPELINE_DEMO_PATH not in sys.path:
        sys.path.insert(0, PIPELINE_DEMO_PATH)
    from demo_data_generator import generate_flights, save_to_csv
    
    num_flights = 500
    context.log.info(f"Generating {num_flights} synthetic flights...")
    
    df = generate_flights(num_flights=num_flights)
    csv_path = save_to_csv(df)
    
    context.log.info(f"Generated {len(df)} flights, saved to {csv_path}")
    
    return Output(
        df,
        metadata={
            "num_flights": len(df),
            "avg_price": float(round(df['totalFare'].mean(), 2)),
            "min_price": float(round(df['totalFare'].min(), 2)),
            "max_price": float(round(df['totalFare'].max(), 2)),
            "unique_routes": int(df.groupby(['startingAirport', 'destinationAirport']).ngroups),
            "csv_path": csv_path,
        }
    )


@asset(
    group_name="full_pipeline",
    description="Ingest flight data to Snowflake DEMO_RAW schema",
    deps=[AssetDep("generate_demo_data")],
)
def ingest_to_snowflake(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> MaterializeResult:
    """Step 2: Ingest data to Snowflake"""
    if PIPELINE_DEMO_PATH not in sys.path:
        sys.path.insert(0, PIPELINE_DEMO_PATH)
    from demo_ingest import run_ingestion
    
    context.log.info("Ingesting data to Snowflake...")
    
    success = run_ingestion()
    
    if not success:
        raise Exception("Snowflake ingestion failed!")
    
    context.log.info("Data ingested to DEMO_RAW.DEMO_FLIGHTS")
    
    return MaterializeResult(
        metadata={
            "rows_ingested": 500,
            "schema": "DEMO_RAW",
            "table": "DEMO_FLIGHTS",
            "status": "success",
        }
    )


@asset(
    group_name="full_pipeline",
    description="Transform raw data through staging, analytics, and ML layers",
    deps=[AssetDep("ingest_to_snowflake")],
)
def transform_data(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Step 3: Run transformations"""
    if PIPELINE_DEMO_PATH not in sys.path:
        sys.path.insert(0, PIPELINE_DEMO_PATH)
    from demo_transform import run_all_transformations
    
    context.log.info("Running transformations...")
    
    success = run_all_transformations()
    
    if not success:
        raise Exception("Transformation failed!")
    
    context.log.info("All transformations complete")
    
    return MaterializeResult(
        metadata={
            "tables_created": 6,
            "staging_table": "DEMO_STAGING.STG_DEMO_FLIGHTS",
            "fact_table": "DEMO_ANALYTICS.DEMO_FACT_FLIGHTS",
            "ml_table": "DEMO_ML.DEMO_ML_FEATURES",
            "status": "success",
        }
    )


@asset(
    group_name="full_pipeline",
    description="Train XGBoost model on demo data with MLflow tracking",
    deps=[AssetDep("transform_data")],
)
def train_demo_model(
    context: AssetExecutionContext,
    mlflow_resource: MLflowResource,
) -> MaterializeResult:
    """Step 4: Train ML model"""
    if PIPELINE_DEMO_PATH not in sys.path:
        sys.path.insert(0, PIPELINE_DEMO_PATH)
    from demo_train import run_training
    
    context.log.info("Training XGBoost model...")
    
    # Configure MLflow
    mlflow_available = mlflow_resource.is_available()
    if mlflow_available:
        import mlflow
        mlflow.set_tracking_uri(mlflow_resource.tracking_uri)
        mlflow.set_experiment(mlflow_resource.experiment_name)
        context.log.info(f"MLflow tracking at {mlflow_resource.tracking_uri}")
    else:
        context.log.warning("MLflow not available, training without tracking")
    
    # Run training
    result = run_training()
    
    if result is None:
        raise Exception("Model training failed!")
    
    metrics = result['metrics']
    context.log.info(f"Model trained - R2: {metrics['r2']:.4f}, RMSE: ${metrics['rmse']:.2f}")
    
    # Log to MLflow
    if mlflow_available:
        import mlflow
        with mlflow.start_run(run_name=f"dagster_training_{datetime.now().strftime('%Y%m%d_%H%M')}"):
            mlflow.log_params({
                "n_estimators": 100,
                "max_depth": 6,
                "learning_rate": 0.1,
                "source": "dagster_pipeline",
            })
            mlflow.log_metrics(metrics)
            mlflow.log_artifact(result['model_path'])
            
            run_id = mlflow.active_run().info.run_id
            context.log.info(f"MLflow run ID: {run_id}")
    
    return MaterializeResult(
        metadata={
            "rmse": float(round(metrics['rmse'], 2)),
            "mae": float(round(metrics['mae'], 2)),
            "r2": float(round(metrics['r2'], 4)),
            "mape": float(round(metrics['mape'], 2)),
            "model_path": result['model_path'],
            "mlflow_enabled": mlflow_available,
        }
    )


@asset(
    group_name="full_pipeline",
    description="Score flights and generate prediction report",
    deps=[AssetDep("train_demo_model")],
)
def batch_predictions(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Step 5: Batch predictions"""
    if PIPELINE_DEMO_PATH not in sys.path:
        sys.path.insert(0, PIPELINE_DEMO_PATH)
    from demo_predict import run_batch_prediction
    
    context.log.info("Running batch predictions...")
    
    result_df = run_batch_prediction(num_flights=100, use_api=False)
    
    if result_df is None:
        raise Exception("Batch prediction failed!")
    
    # Calculate metrics
    rmse = float(np.sqrt(np.mean((result_df['PREDICTED_FARE'] - result_df['ACTUAL_FARE']) ** 2)))
    mae = float(np.mean(np.abs(result_df['PREDICTED_FARE'] - result_df['ACTUAL_FARE'])))
    
    context.log.info(f"Scored {len(result_df)} flights - RMSE: ${rmse:.2f}")
    
    # Create report
    report_md = f"""
## Batch Prediction Report
**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### Summary
- **Flights Scored:** {len(result_df)}
- **RMSE:** ${rmse:.2f}
- **MAE:** ${mae:.2f}
- **Avg Actual Price:** ${float(result_df['ACTUAL_FARE'].mean()):.2f}
- **Avg Predicted Price:** ${float(result_df['PREDICTED_FARE'].mean()):.2f}
"""
    
    return MaterializeResult(
        metadata={
            "flights_scored": len(result_df),
            "rmse": float(round(rmse, 2)),
            "mae": float(round(mae, 2)),
            "avg_actual": float(round(result_df['ACTUAL_FARE'].mean(), 2)),
            "avg_predicted": float(round(result_df['PREDICTED_FARE'].mean(), 2)),
            "report": MetadataValue.md(report_md),
        }
    )


# JOBS

# Full pipeline job
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.groups("full_pipeline"),
    description="Run the complete pipeline: Generate → Ingest → Transform → Train → Predict",
)


# SCHEDULES

# Weekly full pipeline
weekly_pipeline_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 2 * * 0",  # Sunday at 2 AM
    name="weekly_full_pipeline",
    description="Run full pipeline every Sunday at 2 AM",
)


# DEFINITIONS

defs = Definitions(
    assets=[
        generate_demo_data,
        ingest_to_snowflake,
        transform_data,
        train_demo_model,
        batch_predictions,
    ],
    jobs=[
        full_pipeline_job,
    ],
    schedules=[
        weekly_pipeline_schedule,
    ],
    resources={
        "snowflake": SnowflakeResource(),
        "mlflow_resource": MLflowResource(
            tracking_uri="http://localhost:5000",
            experiment_name="flight-pricing-dagster"
        ),
    },
)
