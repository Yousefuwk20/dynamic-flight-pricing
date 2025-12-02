"""
Snowflake Database Connector
Reusable connection utilities for the flight pricing pipeline
"""

import os
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
from contextlib import contextmanager

# Try to load .env from multiple locations
env_paths = [
    Path(__file__).parent.parent / 'config' / '.env',  # Local dev
    Path('/opt/airflow/.env'),                          # Airflow container
    Path(__file__).parent / '.env',                     # Same directory
]

for env_path in env_paths:
    if env_path.exists():
        load_dotenv(env_path)
        break


def get_snowflake_config():
    """Get Snowflake connection config from environment variables"""
    return {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'FLIGHT_PRICING_WH'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'FLIGHT_PRICING'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'RAW'),
    }


@contextmanager
def get_connection(schema: str = None):
    """
    Context manager for Snowflake connections
    
    Usage:
        with get_connection('ANALYTICS') as conn:
            df = pd.read_sql("SELECT * FROM table", conn)
    """
    config = get_snowflake_config()
    if schema:
        config['schema'] = schema
    
    conn = snowflake.connector.connect(**config)
    try:
        yield conn
    finally:
        conn.close()


def execute_query(query: str, schema: str = None):
    """Execute a single query and return results"""
    with get_connection(schema) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()


def execute_query_df(query: str, schema: str = None):
    """Execute query and return as pandas DataFrame"""
    import pandas as pd
    with get_connection(schema) as conn:
        return pd.read_sql(query, conn)


if __name__ == '__main__':
    # Test connection
    try:
        result = execute_query("SELECT CURRENT_TIMESTAMP()")
        print(f"Connected to Snowflake: {result[0][0]}")
    except Exception as e:
        print(f"Connection failed: {e}")
