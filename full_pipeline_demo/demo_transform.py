"""Demo DBT Alternative - Direct SQL Transformations"""

import snowflake.connector
from config import SNOWFLAKE_CONFIG, DEMO_SCHEMAS


def get_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def run_transformation(conn, name: str, sql: str):
    """Run a single transformation and return row count"""
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        table_name = sql.split("AS")[0].split("TABLE")[-1].strip().split()[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"   {name}: {count} rows")
        cursor.close()
        return True
    except Exception as e:
        print(f"   {name}: {e}")
        return False


def run_all_transformations():
    """Run all demo transformations"""
    print("\n" + "=" * 50)
    print("RUNNING TRANSFORMATIONS")
    print("=" * 50)
    
    conn = get_connection()
    
    print("\nStaging...")
    
    stg_demo_flights = f"""
    CREATE OR REPLACE TABLE {DEMO_SCHEMAS['staging']}.STG_DEMO_FLIGHTS AS
    WITH source AS (
        SELECT * FROM {DEMO_SCHEMAS['raw']}.DEMO_FLIGHTS
    ),
    parsed AS (
        SELECT
            "legId" AS leg_id,
            TO_DATE("searchDate") AS search_date,
            TO_DATE("flightDate") AS flight_date,
            UPPER(TRIM("startingAirport")) AS starting_airport,
            UPPER(TRIM("destinationAirport")) AS destination_airport,
            COALESCE("isBasicEconomy", FALSE) AS is_basic_economy,
            COALESCE("isRefundable", FALSE) AS is_refundable,
            COALESCE("isNonStop", FALSE) AS is_non_stop,
            CAST("baseFare" AS DECIMAL(10,2)) AS base_fare,
            CAST("totalFare" AS DECIMAL(10,2)) AS total_fare,
            CAST("seatsRemaining" AS INTEGER) AS seats_remaining,
            CAST("totalTravelDistance" AS DECIMAL(10,2)) AS total_travel_distance,
            CAST("elapsedDays" AS INTEGER) AS elapsed_days,
            CASE 
                WHEN "travelDuration" LIKE 'PT%H%M'
                THEN CAST(REGEXP_SUBSTR("travelDuration", '[0-9]+', 1, 1) AS INTEGER) * 60 
                     + CAST(REGEXP_SUBSTR("travelDuration", '[0-9]+', 1, 2) AS INTEGER)
                ELSE NULL
            END AS travel_duration_minutes,
            "segmentsAirlineName" AS segments_airline_names,
            "segmentsAirlineCode" AS segments_airline_codes,
            "segmentsCabinCode" AS segments_cabin_codes,
            "fareBasisCode" AS fare_basis_code,
            CURRENT_TIMESTAMP() AS loaded_at
        FROM source
    ),
    staged AS (
        SELECT
            *,
            DATEDIFF(DAY, search_date, flight_date) AS days_until_flight,
            ARRAY_SIZE(SPLIT(segments_airline_codes, '||')) AS num_segments,
            SPLIT_PART(segments_airline_codes, '||', 1) AS primary_carrier,
            SPLIT_PART(segments_cabin_codes, '||', 1) AS primary_cabin,
            CAST(total_fare - base_fare AS DECIMAL(10,2)) AS taxes_and_fees
        FROM parsed
    )
    SELECT * FROM staged
    WHERE leg_id IS NOT NULL
      AND search_date IS NOT NULL
      AND flight_date IS NOT NULL
      AND total_fare > 0
      AND seats_remaining >= 0
    """
    run_transformation(conn, "stg_demo_flights", stg_demo_flights)
    
    # DIMENSIONS
    print("\nDimensions...")
    
    dim_airports = f"""
    CREATE OR REPLACE TABLE {DEMO_SCHEMAS['analytics']}.DEMO_DIM_AIRPORTS AS
    WITH airports AS (
        SELECT DISTINCT starting_airport AS code FROM {DEMO_SCHEMAS['staging']}.STG_DEMO_FLIGHTS
        UNION
        SELECT DISTINCT destination_airport AS code FROM {DEMO_SCHEMAS['staging']}.STG_DEMO_FLIGHTS
    )
    SELECT
        code,
        CASE code
            WHEN 'JFK' THEN 'New York'
            WHEN 'LAX' THEN 'Los Angeles'
            WHEN 'ORD' THEN 'Chicago'
            WHEN 'DFW' THEN 'Dallas'
            WHEN 'DEN' THEN 'Denver'
            WHEN 'SFO' THEN 'San Francisco'
            WHEN 'SEA' THEN 'Seattle'
            WHEN 'ATL' THEN 'Atlanta'
            WHEN 'MIA' THEN 'Miami'
            WHEN 'BOS' THEN 'Boston'
            WHEN 'PHX' THEN 'Phoenix'
            WHEN 'LAS' THEN 'Las Vegas'
            ELSE 'Unknown'
        END AS city,
        CASE code
            WHEN 'JFK' THEN 'NY'
            WHEN 'LAX' THEN 'CA'
            WHEN 'ORD' THEN 'IL'
            WHEN 'DFW' THEN 'TX'
            WHEN 'DEN' THEN 'CO'
            WHEN 'SFO' THEN 'CA'
            WHEN 'SEA' THEN 'WA'
            WHEN 'ATL' THEN 'GA'
            WHEN 'MIA' THEN 'FL'
            WHEN 'BOS' THEN 'MA'
            WHEN 'PHX' THEN 'AZ'
            WHEN 'LAS' THEN 'NV'
            ELSE 'XX'
        END AS state,
        CURRENT_TIMESTAMP() AS created_at
    FROM airports
    ORDER BY code
    """
    run_transformation(conn, "demo_dim_airports", dim_airports)
    
    dim_airlines = f"""
    CREATE OR REPLACE TABLE {DEMO_SCHEMAS['analytics']}.DEMO_DIM_AIRLINES AS
    WITH source_data AS (
        SELECT 
            segments_airline_codes AS raw_codes,
            segments_airline_names AS raw_names
        FROM {DEMO_SCHEMAS['staging']}.STG_DEMO_FLIGHTS
        WHERE segments_airline_codes IS NOT NULL
    ),
    flattened AS (
        SELECT
            TRIM(f.value::STRING) AS airline_code,
            TRIM(SPLIT(t.raw_names, '||')[f.index]::STRING) AS airline_name
        FROM source_data t,
        LATERAL FLATTEN(input => SPLIT(t.raw_codes, '||')) f
        WHERE airline_code IS NOT NULL AND airline_code != ''
    ),
    ranked AS (
        SELECT 
            airline_code,
            airline_name,
            COUNT(*) AS frequency,
            ROW_NUMBER() OVER (PARTITION BY airline_code ORDER BY COUNT(*) DESC) AS rn
        FROM flattened
        WHERE airline_name IS NOT NULL AND airline_name != ''
        GROUP BY airline_code, airline_name
    )
    SELECT
        airline_code,
        airline_name,
        CURRENT_TIMESTAMP() AS created_at
    FROM ranked
    WHERE rn = 1
    ORDER BY airline_code
    """
    run_transformation(conn, "demo_dim_airlines", dim_airlines)
    
    dim_date = f"""
    CREATE OR REPLACE TABLE {DEMO_SCHEMAS['analytics']}.DEMO_DIM_DATE AS
    WITH date_spine AS (
        SELECT DISTINCT flight_date AS date_value FROM {DEMO_SCHEMAS['staging']}.STG_DEMO_FLIGHTS
        UNION
        SELECT DISTINCT search_date AS date_value FROM {DEMO_SCHEMAS['staging']}.STG_DEMO_FLIGHTS
    )
    SELECT
        TO_NUMBER(TO_CHAR(date_value, 'YYYYMMDD')) AS date_key,
        date_value AS full_date,
        YEAR(date_value) AS year,
        MONTH(date_value) AS month,
        DAY(date_value) AS day,
        DAYOFWEEK(date_value) AS day_of_week_num,
        DAYNAME(date_value) AS day_name,
        MONTHNAME(date_value) AS month_name,
        QUARTER(date_value) AS quarter,
        WEEKOFYEAR(date_value) AS week_of_year,
        CASE WHEN DAYOFWEEK(date_value) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE 
            WHEN (MONTH(date_value) = 11 AND DAY(date_value) BETWEEN 22 AND 28 AND DAYOFWEEK(date_value) = 4) THEN TRUE
            WHEN (MONTH(date_value) = 12 AND DAY(date_value) IN (24, 25, 31)) THEN TRUE
            WHEN (MONTH(date_value) = 1 AND DAY(date_value) = 1) THEN TRUE
            WHEN (MONTH(date_value) = 7 AND DAY(date_value) = 4) THEN TRUE
            WHEN (MONTH(date_value) = 12 AND DAY(date_value) BETWEEN 20 AND 31) THEN TRUE
            ELSE FALSE
        END AS is_holiday,
        CURRENT_TIMESTAMP() AS created_at
    FROM date_spine
    ORDER BY date_key
    """
    run_transformation(conn, "demo_dim_date", dim_date)
    
    # FACTS
    print("\nFacts...")
    
    fact_flights = f"""
    CREATE OR REPLACE TABLE {DEMO_SCHEMAS['analytics']}.DEMO_FACT_FLIGHTS AS
    SELECT
        starting_airport || '_' || destination_airport AS route_key,
        TO_NUMBER(TO_CHAR(flight_date, 'YYYYMMDD')) AS date_key,
        primary_carrier AS airline_key,
        starting_airport AS origin_airport_key,
        destination_airport AS dest_airport_key,
        leg_id,             
        search_date,         
        flight_date,        
        primary_cabin,
        fare_basis_code,   
        total_fare,
        base_fare,
        taxes_and_fees,    
        CASE 
            WHEN total_travel_distance > 0 
            THEN ROUND(total_fare / total_travel_distance, 4)
            ELSE 0 
        END AS fare_per_mile,
        seats_remaining,
        days_until_flight,
        total_travel_distance,
        travel_duration_minutes,
        num_segments,
        is_basic_economy,
        is_refundable,
        is_non_stop,
        loaded_at
    FROM {DEMO_SCHEMAS['staging']}.STG_DEMO_FLIGHTS
    WHERE total_fare > 0 
      AND days_until_flight >= 0
    """
    run_transformation(conn, "demo_fact_flights", fact_flights)
    
    # ML FEATURES
    print("\nML Features...")
    
    ml_features = f"""
    CREATE OR REPLACE TABLE {DEMO_SCHEMAS['ml']}.DEMO_ML_FEATURES AS
    SELECT
        f.total_fare,
        f.leg_id,
        f.fare_basis_code,
        f.days_until_flight,
        f.seats_remaining,
        f.total_travel_distance,
        f.travel_duration_minutes,
        f.num_segments,
        f.airline_key AS airline_code,
        origin.city AS origin_city,
        dest.city AS dest_city,
        f.date_key AS flight_date_key,
        d.year AS flight_year,
        d.month AS flight_month,
        d.day_of_week_num AS flight_day_of_week,
        CASE WHEN d.is_weekend THEN 1 ELSE 0 END AS is_weekend,
        CASE WHEN d.is_holiday THEN 1 ELSE 0 END AS is_holiday,
        CASE WHEN f.is_basic_economy THEN 1 ELSE 0 END AS is_basic_economy,
        CASE WHEN f.is_non_stop THEN 1 ELSE 0 END AS is_non_stop,
        CASE WHEN f.is_refundable THEN 1 ELSE 0 END AS is_refundable,
        CURRENT_TIMESTAMP() AS created_at
    FROM {DEMO_SCHEMAS['analytics']}.DEMO_FACT_FLIGHTS f
    JOIN {DEMO_SCHEMAS['analytics']}.DEMO_DIM_DATE d 
        ON f.date_key = d.date_key
    JOIN {DEMO_SCHEMAS['analytics']}.DEMO_DIM_AIRLINES al 
        ON f.airline_key = al.airline_code
    JOIN {DEMO_SCHEMAS['analytics']}.DEMO_DIM_AIRPORTS origin 
        ON f.origin_airport_key = origin.code
    JOIN {DEMO_SCHEMAS['analytics']}.DEMO_DIM_AIRPORTS dest 
        ON f.dest_airport_key = dest.code
    WHERE f.total_fare > 0
      AND f.total_fare < 10000
      AND f.days_until_flight BETWEEN 0 AND 90
    """
    run_transformation(conn, "demo_ml_features", ml_features)
    
    cursor = conn.cursor()
    print("\nTransformations complete:")
    for schema, table in [
        (DEMO_SCHEMAS['staging'], 'STG_DEMO_FLIGHTS'),
        (DEMO_SCHEMAS['analytics'], 'DEMO_DIM_AIRPORTS'),
        (DEMO_SCHEMAS['analytics'], 'DEMO_DIM_AIRLINES'),
        (DEMO_SCHEMAS['analytics'], 'DEMO_DIM_DATE'),
        (DEMO_SCHEMAS['analytics'], 'DEMO_FACT_FLIGHTS'),
        (DEMO_SCHEMAS['ml'], 'DEMO_ML_FEATURES'),
    ]:
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        print(f"   {table}: {cursor.fetchone()[0]} rows")
    
    cursor.close()
    conn.close()
    return True


if __name__ == '__main__':
    run_all_transformations()
