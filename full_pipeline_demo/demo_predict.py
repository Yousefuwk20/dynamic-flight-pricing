"""Demo Batch Prediction - Score flights using trained model"""

import pandas as pd
import numpy as np
import snowflake.connector
import xgboost as xgb
import json
import pickle
import requests
from datetime import datetime, timedelta
from pathlib import Path

from config import (
    SNOWFLAKE_CONFIG, DEMO_SCHEMAS,
    ML_CONFIG, API_CONFIG, BASE_DIR
)


def load_demo_model():
    """Load the demo-trained model and encoders"""
    model_dir = BASE_DIR / 'models'
    model_path = model_dir / ML_CONFIG['model_file']
    
    if not model_path.exists():
        print(f"   Model not found - run demo_train.py first")
        return None, None, None
    
    model = xgb.XGBRegressor()
    model.load_model(str(model_path))
    
    with open(model_dir / 'demo_model_metadata.json', 'r') as f:
        metadata = json.load(f)
    with open(model_dir / 'demo_encoders.pkl', 'rb') as f:
        encoders = pickle.load(f)
    
    print(f"   Model loaded")
    return model, encoders, metadata


def get_flights_to_score(limit: int = 100):
    """Fetch flights from Snowflake to score"""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    
    query = f"""
    SELECT leg_id, total_fare as actual_fare, days_until_flight, seats_remaining,
           total_travel_distance, travel_duration_minutes, num_segments, airline_code,
           origin_city, dest_city, flight_month, flight_day_of_week, is_weekend,
           is_holiday, is_basic_economy, is_non_stop, is_refundable
    FROM {DEMO_SCHEMAS['ml']}.DEMO_ML_FEATURES
    ORDER BY RANDOM() LIMIT {limit}
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    df.columns = [c.upper() for c in df.columns]
    print(f"   {len(df)} flights to score")
    return df


def score_with_local_model(df: pd.DataFrame, model, encoders, feature_names):
    """Score flights using locally trained model"""
    numeric_cols = [
        'DAYS_UNTIL_FLIGHT', 'SEATS_REMAINING', 'TOTAL_TRAVEL_DISTANCE',
        'TRAVEL_DURATION_MINUTES', 'NUM_SEGMENTS', 'FLIGHT_MONTH',
        'FLIGHT_DAY_OF_WEEK', 'IS_WEEKEND', 'IS_HOLIDAY',
        'IS_BASIC_ECONOMY', 'IS_NON_STOP', 'IS_REFUNDABLE'
    ]
    categorical_cols = ['AIRLINE_CODE', 'ORIGIN_CITY', 'DEST_CITY']
    
    X = df[numeric_cols].copy()
    for col in categorical_cols:
        mapping = encoders[col]['mapping']
        X[col] = df[col].map(lambda x: mapping.get(str(x), 0))
    
    return model.predict(X[numeric_cols + categorical_cols].values)


def score_with_api(df: pd.DataFrame):
    """Score flights using the ML API"""
    print("\nScoring with ML API...")
    
    predictions = []
    
    try:
        response = requests.get(f"{API_CONFIG['url']}/health", timeout=5)
        if response.status_code != 200:
            print("   API not available")
            return None
    except:
        print("   API not reachable")
        return None
    
    for idx, row in df.iterrows():
        try:
            flight_date = datetime.now() + timedelta(days=int(row['DAYS_UNTIL_FLIGHT']))
            
            payload = {
                "searchDate": datetime.now().strftime('%Y-%m-%d'),
                "flightDate": flight_date.strftime('%Y-%m-%d'),
                "startingAirport": row['ORIGIN_CITY'][:3].upper(),  # Use city code
                "destinationAirport": row['DEST_CITY'][:3].upper(),
                "seatsRemaining": int(row['SEATS_REMAINING']),
                "totalTravelDistance": float(row['TOTAL_TRAVEL_DISTANCE']),
                "durationMinutes": int(row['TRAVEL_DURATION_MINUTES']),
                "numSegments": int(row['NUM_SEGMENTS']),
                "carrier": row['AIRLINE_CODE'],
                "isBasicEconomy": bool(row['IS_BASIC_ECONOMY']),
                "isNonStop": bool(row['IS_NON_STOP']),
                "isRefundable": bool(row['IS_REFUNDABLE']),
                "isHoliday": bool(row['IS_HOLIDAY']),
            }
            
            response = requests.post(f"{API_CONFIG['url']}/predict", json=payload, timeout=10)
            result = response.json()
            predictions.append(result.get('dynamic_price', result.get('ml_base_price', 0)))
            
        except Exception as e:
            predictions.append(None)
    
    return predictions


def generate_scoring_report(df: pd.DataFrame, predictions: np.ndarray, method: str):
    """Generate scoring report with comparison to actual prices"""
    df = df.copy()
    df['PREDICTED_FARE'] = predictions
    df['PREDICTION_ERROR'] = df['PREDICTED_FARE'] - df['ACTUAL_FARE']
    df['PREDICTION_PCT_ERROR'] = (df['PREDICTION_ERROR'] / df['ACTUAL_FARE']) * 100
    
    valid = df.dropna(subset=['PREDICTED_FARE'])
    rmse = np.sqrt(np.mean(valid['PREDICTION_ERROR'] ** 2))
    mae = np.mean(np.abs(valid['PREDICTION_ERROR']))
    mape = np.mean(np.abs(valid['PREDICTION_PCT_ERROR']))
    
    print(f"\nMetrics: RMSE=${rmse:.2f}, MAE=${mae:.2f}, MAPE={mape:.2f}%")
    print(f"   Avg Actual: ${valid['ACTUAL_FARE'].mean():.2f} | Predicted: ${valid['PREDICTED_FARE'].mean():.2f}")
    
    samples = [f"{r['ORIGIN_CITY'][:3]}>{r['DEST_CITY'][:3]} ${r['PREDICTED_FARE']:.0f}" for _, r in valid.head(5).iterrows()]
    print(f"\n   Sample: {', '.join(samples)}")
    
    return df


def save_predictions(df: pd.DataFrame, output_file: str = 'demo_predictions.csv'):
    """Save predictions to CSV"""
    output_path = BASE_DIR / 'output' / output_file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"   Saved to {output_path}")
    return output_path


def run_batch_prediction(num_flights: int = 100, use_api: bool = False):
    """Run batch prediction pipeline"""
    print("\n" + "=" * 50)
    print("BATCH PREDICTION")
    print("=" * 50)
    
    try:
        if not use_api:
            model, encoders, metadata = load_demo_model()
            if model is None:
                use_api = True
        
        df = get_flights_to_score(limit=num_flights)
        if len(df) == 0:
            print("No flights found!")
            return None
        
        if use_api:
            predictions = score_with_api(df)
            if predictions is None:
                return None
        else:
            predictions = score_with_local_model(df, model, encoders, metadata['feature_names'])
        
        result_df = generate_scoring_report(df, predictions, "API" if use_api else "Local")
        save_predictions(result_df)
        
        print(f"\nBatch prediction complete!")
        return result_df
        
    except Exception as e:
        print(f"\nPrediction failed: {e}")
        return None


if __name__ == '__main__':
    import sys
    
    num_flights = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    use_api = '--api' in sys.argv
    
    run_batch_prediction(num_flights=num_flights, use_api=use_api)
