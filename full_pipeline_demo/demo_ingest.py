"""
Demo Data Ingestion to Snowflake
================================
Uploads generated demo flight data to Snowflake DEMO_RAW schema.
Creates necessary schemas and tables if they don't exist.
"""

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path
import sys

from config import SNOWFLAKE_CONFIG, DEMO_SCHEMAS, DEMO_TABLES, BASE_DIR


def get_snowflake_connection():
    """Create Snowflake connection"""
    print("Connecting to Snowflake...")
    
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_CONFIG['account'],
        user=SNOWFLAKE_CONFIG['user'],
        password=SNOWFLAKE_CONFIG['password'],
        warehouse=SNOWFLAKE_CONFIG['warehouse'],
        database=SNOWFLAKE_CONFIG['database'],
        role=SNOWFLAKE_CONFIG['role'],
    )
    
    print(f"Connected to {SNOWFLAKE_CONFIG['account']}")
    return conn


def setup_demo_schemas(conn):
    """Create demo schemas if they don't exist"""
    print("\nSetting up demo schemas...")
    
    cursor = conn.cursor()
    
    for schema_name, schema_value in DEMO_SCHEMAS.items():
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_value}")
            print(f"   Schema {schema_value} ready")
        except Exception as e:
            print(f"   Schema {schema_value}: {e}")
    
    cursor.close()


def create_raw_table(conn):
    """Create the demo_flights table in DEMO_RAW schema"""
    print(f"\nCreating raw table {DEMO_SCHEMAS['raw']}.{DEMO_TABLES['raw_flights']}...")
    
    cursor = conn.cursor()
    
    cursor.execute(f"DROP TABLE IF EXISTS {DEMO_SCHEMAS['raw']}.{DEMO_TABLES['raw_flights']}")
    
    # Create table matching the original raw data structure
    create_sql = f"""
    CREATE TABLE {DEMO_SCHEMAS['raw']}.{DEMO_TABLES['raw_flights']} (
        "legId" VARCHAR(100),
        "searchDate" DATE,
        "flightDate" DATE,
        "startingAirport" VARCHAR(10),
        "destinationAirport" VARCHAR(10),
        "isBasicEconomy" BOOLEAN,
        "isRefundable" BOOLEAN,
        "isNonStop" BOOLEAN,
        "baseFare" DECIMAL(10,2),
        "totalFare" DECIMAL(10,2),
        "seatsRemaining" INTEGER,
        "totalTravelDistance" DECIMAL(10,2),
        "elapsedDays" INTEGER,
        "travelDuration" VARCHAR(50),
        "fareBasisCode" VARCHAR(50),
        "segmentsDepartureTimeEpochSeconds" VARCHAR(500),
        "segmentsDepartureTimeRaw" VARCHAR(1000),
        "segmentsArrivalTimeEpochSeconds" VARCHAR(500),
        "segmentsArrivalTimeRaw" VARCHAR(1000),
        "segmentsArrivalAirportCode" VARCHAR(100),
        "segmentsDepartureAirportCode" VARCHAR(100),
        "segmentsAirlineName" VARCHAR(500),
        "segmentsAirlineCode" VARCHAR(100),
        "segmentsEquipmentDescription" VARCHAR(500),
        "segmentsDurationInSeconds" VARCHAR(200),
        "segmentsDistance" VARCHAR(200),
        "segmentsCabinCode" VARCHAR(100),
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """
    
    cursor.execute(create_sql)
    print(f"   Table created")
    cursor.close()


def load_demo_data(conn, csv_path: str = None):
    """Load demo flight data into Snowflake"""
    if csv_path is None:
        csv_path = BASE_DIR / 'data' / 'demo_flights.csv'
    
    if not Path(csv_path).exists():
        print(f"CSV not found: {csv_path}")
        return False
    
    print(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path)
    df['searchDate'] = pd.to_datetime(df['searchDate']).dt.date
    df['flightDate'] = pd.to_datetime(df['flightDate']).dt.date
    
    cursor = conn.cursor()
    cursor.execute(f"USE SCHEMA {DEMO_SCHEMAS['raw']}")
    
    success, num_chunks, num_rows, output = write_pandas(
        conn=conn,
        df=df,
        table_name=DEMO_TABLES['raw_flights'],
        schema=DEMO_SCHEMAS['raw'],
        quote_identifiers=True,
    )
    
    if success:
        print(f"   Uploaded {num_rows} rows")
    else:
        print(f"   Upload failed: {output}")
        return False
    
    cursor.close()
    return True


def show_sample_data(conn):
    """Display sample of uploaded data"""
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT "startingAirport", "destinationAirport", "totalFare", "seatsRemaining"
        FROM {DEMO_SCHEMAS['raw']}.{DEMO_TABLES['raw_flights']}
        LIMIT 3
    """)
    rows = cursor.fetchall()
    print("\nSample: " + ", ".join([f"{r[0]}→{r[1]} ${r[2]:.0f}" for r in rows]))
    cursor.close()


def run_ingestion(csv_path: str = None):
    """Run the full ingestion process"""
    print("\n" + "=" * 50)
    print("INGESTING DATA TO SNOWFLAKE")
    print("=" * 50)
    
    try:
        conn = get_snowflake_connection()
        setup_demo_schemas(conn)
        create_raw_table(conn)
        success = load_demo_data(conn, csv_path)
        
        if success:
            show_sample_data(conn)
            print(f"\nData loaded to {DEMO_SCHEMAS['raw']}.{DEMO_TABLES['raw_flights']}")
        
        conn.close()
        return success
        
    except Exception as e:
        print(f"\nError: {e}")
        return False


if __name__ == '__main__':
    csv_path = sys.argv[1] if len(sys.argv) > 1 else None
    run_ingestion(csv_path)
