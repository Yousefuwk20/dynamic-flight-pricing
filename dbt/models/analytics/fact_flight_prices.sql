{{
    config(
        materialized='table',
        tags=['facts', 'pricing'],
        cluster_by=['date_key', 'airline_key'] 
    )
}}

SELECT
    -- Route Key
    starting_airport || '_' || destination_airport as route_key,
    -- Date Key
    TO_NUMBER(TO_CHAR(flight_date, 'YYYYMMDD')) as date_key,
    -- Airline Key
    primary_carrier as airline_key,
    -- Airport Keys
    starting_airport as origin_airport_key,
    destination_airport as dest_airport_key,

    leg_id,             
    search_date,         
    flight_date,        
    primary_cabin,
    fare_basis_code,   
    
    -- Pricing
    total_fare,
    base_fare,
    taxes_and_fees,    
    CASE 
        WHEN total_travel_distance > 0 THEN ROUND(total_fare / total_travel_distance, 4)
        ELSE 0 
    END as fare_per_mile,

    -- Inventory & Demand
    seats_remaining,
    days_until_flight,
    
    -- Flight Stats
    total_travel_distance,
    travel_duration_minutes,
    num_segments,

    -- BOOLEAN FLAGS
    is_basic_economy,
    is_refundable,
    is_non_stop

FROM {{ ref('staging_model') }}

WHERE total_fare > 0 
  AND days_until_flight >= 0