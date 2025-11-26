"""
Snowflake Connection and Query Utilities
"""
import snowflake.connector
import pandas as pd
import polars as pl
from typing import List, Dict, Optional
from contextlib import contextmanager
from config import snowflake_config

@contextmanager
def get_snowflake_connection():
    """Get Snowflake connection context manager"""
    conn = snowflake.connector.connect(
        account=snowflake_config.account,
        user=snowflake_config.user,
        password=snowflake_config.password,
        warehouse=snowflake_config.warehouse,
        database=snowflake_config.database,
    )
    try:
        yield conn
    finally:
        conn.close()


def execute_query(query: str, params: dict = None) -> pd.DataFrame:
    """Execute a query and return results as DataFrame"""
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        finally:
            cursor.close()


def execute_statement(statement: str):
    """Execute a statement (INSERT, UPDATE, etc.)"""
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(statement)
            conn.commit()
        finally:
            cursor.close()


def write_dataframe(df: pd.DataFrame, table_name: str, schema: str, if_exists: str = 'append'):
    """Write DataFrame to Snowflake table"""
    from snowflake.connector.pandas_tools import write_pandas
    
    with get_snowflake_connection() as conn:
        # Create table if it doesn't exist
        if if_exists == 'replace':
            conn.cursor().execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            schema=schema,
            auto_create_table=True,
            overwrite=(if_exists == 'replace')
        )
        
        return success


def get_ml_features_for_scoring(days_ahead: int = 30) -> pd.DataFrame:
    """Get upcoming flights that need price predictions"""
    query = f"""
    SELECT 
        f.FLIGHT_ID,
        f.DAYS_UNTIL_FLIGHT,
        f.SEATS_REMAINING,
        f.TOTAL_TRAVEL_DISTANCE,
        f.TRAVEL_DURATION_MINUTES,
        f.NUM_SEGMENTS,
        f.AIRLINE_CODE,
        f.ORIGIN_CITY,
        f.DEST_CITY,
        f.FLIGHT_YEAR,
        f.FLIGHT_MONTH,
        f.FLIGHT_DAY_OF_WEEK,
        f.IS_WEEKEND,
        f.IS_HOLIDAY,
        f.IS_BASIC_ECONOMY,
        f.IS_NON_STOP,
        f.IS_REFUNDABLE,
        f.CABIN_CATEGORY,
        f.FARE_RULE_NUMBER,
        f.PASSENGER_TYPE,
        f.SEASONALITY_PROXY,
        f.HAS_NUMERIC_RULE,
        f.IS_NIGHT_FARE_PROXY,
        f.IS_WEEKEND_FARE_PROXY,
        f.TOTAL_SEATS,
        f.CURRENT_PRICE
    FROM {snowflake_config.analytics_schema}.FACT_ML_FEATURES f
    WHERE f.DAYS_UNTIL_FLIGHT BETWEEN 0 AND {days_ahead}
      AND f.FLIGHT_DATE >= CURRENT_DATE()
    ORDER BY f.FLIGHT_DATE, f.FLIGHT_ID
    """
    return execute_query(query)


def get_training_data_raw(sample_pct: int = 30) -> pl.DataFrame:
    """
    Get training data from ML_FLIGHT_PRICING table (matches notebook query)
    Returns Polars DataFrame for efficient processing
    """
    with get_snowflake_connection() as conn:
        cursor = conn.cursor()
        
        query = f"SELECT * FROM {snowflake_config.analytics_schema}.ML_FLIGHT_PRICING SAMPLE({sample_pct})"
        
        cursor.execute(query)
        
        arrow_table = cursor.fetch_arrow_all()
        
        df = pl.from_arrow(arrow_table)
        
        if 'FLIGHT_DATE_KEY' in df.columns:
            df = df.drop('FLIGHT_DATE_KEY')
        
        cursor.close()
        
        return df


def get_training_data(months_back: int = 12) -> pd.DataFrame:
    """Get historical data for model training (legacy pandas version)"""
    query = f"""
    SELECT 
        f.DAYS_UNTIL_FLIGHT,
        f.SEATS_REMAINING,
        f.TOTAL_TRAVEL_DISTANCE,
        f.TRAVEL_DURATION_MINUTES,
        f.NUM_SEGMENTS,
        f.AIRLINE_CODE,
        f.ORIGIN_CITY,
        f.DEST_CITY,
        f.FLIGHT_YEAR,
        f.FLIGHT_MONTH,
        f.FLIGHT_DAY_OF_WEEK,
        f.IS_WEEKEND,
        f.IS_HOLIDAY,
        f.IS_BASIC_ECONOMY,
        f.IS_NON_STOP,
        f.IS_REFUNDABLE,
        f.CABIN_CATEGORY,
        f.FARE_RULE_NUMBER,
        f.PASSENGER_TYPE,
        f.SEASONALITY_PROXY,
        f.HAS_NUMERIC_RULE,
        f.IS_NIGHT_FARE_PROXY,
        f.IS_WEEKEND_FARE_PROXY,
        f.TOTAL_FARE as TARGET_PRICE
    FROM {snowflake_config.analytics_schema}.FACT_ML_FEATURES f
    WHERE f.SEARCH_DATE >= DATEADD(month, -{months_back}, CURRENT_DATE())
      AND f.TOTAL_FARE IS NOT NULL
      AND f.TOTAL_FARE > 0
    """
    return execute_query(query)


def get_competitor_prices(flight_ids: List[str]) -> Dict[str, List[float]]:
    """Get competitor prices for given flights (if you have this data)"""
    return {}


def save_predictions(predictions_df: pd.DataFrame):
    """Save ML predictions to Snowflake"""
    predictions_df['PREDICTION_TIMESTAMP'] = pd.Timestamp.now()
    predictions_df['MODEL_VERSION'] = get_current_model_version()
    
    write_dataframe(
        df=predictions_df,
        table_name='FLIGHT_PRICES',
        schema=snowflake_config.ml_schema,
        if_exists='append'
    )


def get_current_model_version() -> str:
    """Get current deployed model version"""
    query = f"""
    SELECT MODEL_VERSION 
    FROM {snowflake_config.ml_schema}.MODEL_REGISTRY 
    WHERE IS_ACTIVE = TRUE
    ORDER BY DEPLOYED_AT DESC
    LIMIT 1
    """
    try:
        result = execute_query(query)
        return result['MODEL_VERSION'].iloc[0] if len(result) > 0 else 'v1.0.0'
    except:
        return 'v1.0.0'


def register_model(model_version: str, metrics: dict, s3_path: str):
    """Register a new model version in Snowflake"""
    import json
    
    execute_statement(f"""
        UPDATE {snowflake_config.ml_schema}.MODEL_REGISTRY 
        SET IS_ACTIVE = FALSE 
        WHERE IS_ACTIVE = TRUE
    """)
    
    execute_statement(f"""
        INSERT INTO {snowflake_config.ml_schema}.MODEL_REGISTRY 
        (MODEL_VERSION, METRICS, S3_PATH, IS_ACTIVE, DEPLOYED_AT)
        VALUES ('{model_version}', '{json.dumps(metrics)}', '{s3_path}', TRUE, CURRENT_TIMESTAMP())
    """)
