"""
Full Pipeline Demo Configuration
================================
Central configuration for the end-to-end flight pricing demo pipeline.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / 'pipeline' / '.env'
load_dotenv(env_path)

# PATHS
BASE_DIR = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / 'dbt'
MODEL_DIR = BASE_DIR.parent / 'api'

# SNOWFLAKE CONFIGURATION
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT', ''),
    'user': os.getenv('SNOWFLAKE_USER', ''),
    'password': os.getenv('SNOWFLAKE_PASSWORD', ''),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'FLIGHTS_WH'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'FLIGHT_PRICING'),
    'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
}

# DEMO SCHEMA CONFIGURATION
DEMO_SCHEMAS = {
    'raw': 'DEMO_RAW',          
    'staging': 'DEMO_STAGING',   
    'analytics': 'DEMO_ANALYTICS', 
    'ml': 'DEMO_ML',             
}

DEMO_TABLES = {
    'raw_flights': 'DEMO_FLIGHTS',
    'staging': 'STG_DEMO_FLIGHTS',
    'dim_airports': 'DIM_AIRPORTS',
    'dim_airlines': 'DIM_AIRLINES',
    'dim_date': 'DIM_DATE',
    'fact_flights': 'FACT_FLIGHT_PRICES',
    'ml_features': 'ML_FLIGHT_FEATURES',
}

# DATA GENERATION CONFIGURATION
DATA_CONFIG = {
    'num_flights': 1000,         
    'date_range_days': 60,       
    'search_date_range': 30,     
}

# Major US airports for demo
AIRPORTS = {
    'JFK': {'city': 'New York', 'state': 'NY'},
    'LAX': {'city': 'Los Angeles', 'state': 'CA'},
    'ORD': {'city': 'Chicago', 'state': 'IL'},
    'DFW': {'city': 'Dallas', 'state': 'TX'},
    'DEN': {'city': 'Denver', 'state': 'CO'},
    'SFO': {'city': 'San Francisco', 'state': 'CA'},
    'SEA': {'city': 'Seattle', 'state': 'WA'},
    'ATL': {'city': 'Atlanta', 'state': 'GA'},
    'MIA': {'city': 'Miami', 'state': 'FL'},
    'BOS': {'city': 'Boston', 'state': 'MA'},
    'PHX': {'city': 'Phoenix', 'state': 'AZ'},
    'LAS': {'city': 'Las Vegas', 'state': 'NV'},
}

# Airlines for demo
AIRLINES = {
    'AA': 'American Airlines',
    'DL': 'Delta Air Lines',
    'UA': 'United Airlines',
    'WN': 'Southwest Airlines',
    'B6': 'JetBlue Airways',
    'AS': 'Alaska Airlines',
    'NK': 'Spirit Airlines',
    'F9': 'Frontier Airlines',
}

# Route distances (approximate miles)
ROUTE_DISTANCES = {
    ('JFK', 'LAX'): 2475,
    ('JFK', 'SFO'): 2586,
    ('JFK', 'MIA'): 1089,
    ('JFK', 'ORD'): 740,
    ('LAX', 'SFO'): 337,
    ('LAX', 'SEA'): 954,
    ('ORD', 'DEN'): 888,
    ('ORD', 'MIA'): 1197,
    ('ATL', 'DFW'): 731,
    ('ATL', 'MIA'): 594,
    ('DEN', 'PHX'): 602,
    ('DEN', 'LAS'): 628,
    ('SFO', 'SEA'): 679,
    ('BOS', 'DFW'): 1551,
    ('BOS', 'MIA'): 1258,
}

# ML CONFIGURATION
ML_CONFIG = {
    'model_file': 'demo_flight_pricing_model.json',
    'test_size': 0.2,
    'random_state': 42,
    'xgboost_params': {
        'n_estimators': 100,
        'max_depth': 6,
        'learning_rate': 0.1,
        'objective': 'reg:squarederror',
    }
}

# API CONFIGURATION
API_CONFIG = {
    'url': os.getenv('ML_API_ENDPOINT', 'http://localhost:8000'),
}

# HOLIDAYS (2024-2025)
HOLIDAYS = [
    '2024-11-28', '2024-11-29',  # Thanksgiving
    '2024-12-24', '2024-12-25',  # Christmas
    '2024-12-31', '2025-01-01',  # New Year
    '2025-01-20',                 # MLK Day
    '2025-02-17',                 # Presidents Day
    '2025-05-26',                 # Memorial Day
    '2025-07-04',                 # July 4th
    '2025-09-01',                 # Labor Day
    '2025-11-27', '2025-11-28',  # Thanksgiving 2025
    '2025-12-24', '2025-12-25',  # Christmas 2025
]
