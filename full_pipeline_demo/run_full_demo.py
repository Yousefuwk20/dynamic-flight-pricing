"""Full Pipeline Demo - End-to-End Orchestrator"""

import argparse
import sys
import time
from datetime import datetime

from config import BASE_DIR, SNOWFLAKE_CONFIG, DEMO_SCHEMAS, DATA_CONFIG

def print_step(step_num: int, total: int, title: str):
    print(f"\n{'='*50}\nSTEP {step_num}/{total}: {title}\n{'='*50}")


def step_1_generate_data(num_flights: int):
    print_step(1, 5, "GENERATE DATA")
    from demo_data_generator import generate_flights, save_to_csv
    df = generate_flights(num_flights=num_flights)
    return True, save_to_csv(df)


def step_2_ingest_to_snowflake(csv_path: str = None):
    print_step(2, 5, "INGEST TO SNOWFLAKE")
    from demo_ingest import run_ingestion
    return run_ingestion(csv_path), None


def step_3_run_dbt():
    print_step(3, 5, "TRANSFORM")
    from demo_transform import run_all_transformations
    return run_all_transformations(), None


def step_4_train_model():
    print_step(4, 5, "TRAIN MODEL")
    from demo_train import run_training
    result = run_training()
    return result is not None, result


def step_5_batch_predict(num_predictions: int = 100):
    print_step(5, 5, "PREDICT")
    from demo_predict import run_batch_prediction
    result = run_batch_prediction(num_flights=num_predictions)
    return result is not None, result


def run_full_pipeline(args):
    """Run the complete pipeline"""
    start_time = time.time()
    
    print("\n" + "#" * 50)
    print("FLIGHT PRICING PIPELINE DEMO")
    print("#" * 50)
    print(f"\nFlights: {args.flights} | DB: {SNOWFLAKE_CONFIG['database']}")
    print("Flow: Generate → Ingest → Transform → Train → Predict\n")
    
    results = {}
    
    if not args.predict_only:
        success, _ = step_1_generate_data(args.flights)
        results['generate'] = success
        if not success: return False
        
        success, _ = step_2_ingest_to_snowflake()
        results['ingest'] = success
        if not success: return False
        
        if not args.skip_dbt:
            success, _ = step_3_run_dbt()
            results['transform'] = success
        
        success, _ = step_4_train_model()
        results['train'] = success
        if not success: return False
    
    success, _ = step_5_batch_predict(num_predictions=min(args.flights, 100))
    results['predict'] = success
    
    elapsed = time.time() - start_time
    
    print("\n" + "#" * 50)
    print(f"PIPELINE COMPLETE ({elapsed:.1f}s)")
    print("#" * 50)
    print(f"\nResults: {' | '.join([f'{k}: {chr(43) if v else chr(45)}' for k, v in results.items()])}")
    
    return all(results.values())


def main():
    parser = argparse.ArgumentParser(description='Run flight pricing pipeline demo')
    parser.add_argument('--flights', '-f', type=int, default=DATA_CONFIG['num_flights'], help='Number of flights')
    parser.add_argument('--skip-dbt', action='store_true', help='Skip transformation step')
    parser.add_argument('--predict-only', action='store_true', help='Only run prediction')
    
    args = parser.parse_args()
    success = run_full_pipeline(args)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
