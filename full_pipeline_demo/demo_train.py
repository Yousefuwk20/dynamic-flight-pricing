"""Demo Model Training - XGBoost with MLflow tracking"""

import pandas as pd
import numpy as np
import snowflake.connector
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import xgboost as xgb
import json
import pickle
from datetime import datetime
from pathlib import Path

from config import (
    SNOWFLAKE_CONFIG, DEMO_SCHEMAS, 
    ML_CONFIG, BASE_DIR
)

try:
    import mlflow
    import mlflow.xgboost
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False


def get_training_data():
    """Fetch ML features from Snowflake"""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    
    query = f"""
    SELECT total_fare, days_until_flight, seats_remaining, total_travel_distance,
           travel_duration_minutes, num_segments, airline_code, origin_city, dest_city,
           flight_month, flight_day_of_week, is_weekend, is_holiday,
           is_basic_economy, is_non_stop, is_refundable
    FROM {DEMO_SCHEMAS['ml']}.DEMO_ML_FEATURES WHERE total_fare > 0
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"   Loaded {len(df)} records (${df['TOTAL_FARE'].min():.0f}-${df['TOTAL_FARE'].max():.0f})")
    return df


def prepare_features(df: pd.DataFrame):
    """Prepare features for XGBoost training"""
    target = 'TOTAL_FARE'
    y = df[target].values
    
    categorical_cols = ['AIRLINE_CODE', 'ORIGIN_CITY', 'DEST_CITY']
    numeric_cols = [
        'DAYS_UNTIL_FLIGHT', 'SEATS_REMAINING', 'TOTAL_TRAVEL_DISTANCE',
        'TRAVEL_DURATION_MINUTES', 'NUM_SEGMENTS', 'FLIGHT_MONTH',
        'FLIGHT_DAY_OF_WEEK', 'IS_WEEKEND', 'IS_HOLIDAY',
        'IS_BASIC_ECONOMY', 'IS_NON_STOP', 'IS_REFUNDABLE'
    ]
    
    encoders = {}
    X_encoded = df[numeric_cols].copy()
    
    for col in categorical_cols:
        le = LabelEncoder()
        X_encoded[col] = le.fit_transform(df[col].fillna('UNKNOWN'))
        encoders[col] = {
            'classes': le.classes_.tolist(),
            'mapping': {str(c): int(i) for i, c in enumerate(le.classes_)}
        }
    
    feature_names = numeric_cols + categorical_cols
    X = X_encoded[feature_names].values
    print(f"   Prepared {X.shape[1]} features")
    return X, y, feature_names, encoders


def train_model(X, y, feature_names):
    """Train XGBoost model"""
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=ML_CONFIG['test_size'], random_state=ML_CONFIG['random_state']
    )
    print(f"   Train: {len(X_train)}, Test: {len(X_test)}")
    
    model = xgb.XGBRegressor(**ML_CONFIG['xgboost_params'], random_state=ML_CONFIG['random_state'])
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
    
    y_pred = model.predict(X_test)
    metrics = {
        'rmse': float(np.sqrt(mean_squared_error(y_test, y_pred))),
        'mae': float(mean_absolute_error(y_test, y_pred)),
        'r2': float(r2_score(y_test, y_pred)),
        'mape': float(np.mean(np.abs((y_test - y_pred) / y_test)) * 100),
    }
    
    print(f"   R²: {metrics['r2']:.4f}, RMSE: ${metrics['rmse']:.2f}, MAPE: {metrics['mape']:.2f}%")
    
    importance = dict(zip(feature_names, model.feature_importances_.tolist()))
    return model, metrics, importance, (X_test, y_test, y_pred)


def save_model(model, encoders, feature_names, metrics):
    """Save model and artifacts"""
    output_dir = BASE_DIR / 'models'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    model_path = output_dir / ML_CONFIG['model_file']
    model.save_model(str(model_path))
    
    metadata = {
        'feature_names': feature_names, 'encoders': encoders, 'metrics': metrics,
        'training_date': datetime.now().isoformat(), 'model_type': 'XGBRegressor',
    }
    with open(output_dir / 'demo_model_metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    
    with open(output_dir / 'demo_encoders.pkl', 'wb') as f:
        pickle.dump(encoders, f)
    
    print(f"   Saved to {output_dir}")
    return model_path, output_dir / 'demo_model_metadata.json'


def log_to_mlflow(model, metrics, feature_importance, params):
    """Log experiment to MLflow"""
    if not MLFLOW_AVAILABLE:
        return
    try:
        mlflow.set_experiment("demo-flight-pricing")
        with mlflow.start_run(run_name=f"demo_{datetime.now().strftime('%Y%m%d_%H%M')}"):
            mlflow.log_params(params)
            mlflow.log_metrics(metrics)
            mlflow.xgboost.log_model(model, "model")
            print("   Logged to MLflow")
    except Exception as e:
        print(f"   MLflow error: {e}")


def run_training():
    """Run the full training pipeline"""
    print("\n" + "=" * 50)
    print("TRAINING MODEL")
    print("=" * 50)
    
    try:
        df = get_training_data()
        if len(df) == 0:
            print("No training data found!")
            return None
        
        X, y, feature_names, encoders = prepare_features(df)
        model, metrics, importance, test_data = train_model(X, y, feature_names)
        model_path, _ = save_model(model, encoders, feature_names, metrics)
        log_to_mlflow(model, metrics, importance, ML_CONFIG['xgboost_params'])
        
        print(f"\nTraining complete!")
        return {'model': model, 'metrics': metrics, 'model_path': str(model_path)}
        
    except Exception as e:
        print(f"\nTraining failed: {e}")
        return None


if __name__ == '__main__':
    run_training()
