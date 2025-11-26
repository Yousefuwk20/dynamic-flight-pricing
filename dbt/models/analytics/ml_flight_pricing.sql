{{
    config(
        materialized='table',
        tags=['ml', 'pricing'],
        cluster_by=['flight_date_key'] 
    )
}}

SELECT
    --Target
    f.total_fare,

    f.fare_basis_code,
    f.days_until_flight,
    f.seats_remaining,
    f.total_travel_distance,
    f.travel_duration_minutes,
    f.num_segments,


    -- Airline
    f.airline_key as airline_code,
    
    -- Route / Airports
    origin.city as origin_city,
    dest.city as dest_city,

    f.date_key as flight_date_key,
    d.year as flight_year,
    d.month as flight_month,
    d.day_of_week_num as flight_day_of_week, -- 0=Sun
    
    CASE WHEN d.is_weekend THEN 1 ELSE 0 END as is_weekend,
    CASE WHEN d.is_holiday THEN 1 ELSE 0 END as is_holiday,

    CASE WHEN f.is_basic_economy THEN 1 ELSE 0 END as is_basic_economy,
    CASE WHEN f.is_non_stop THEN 1 ELSE 0 END as is_non_stop,
    CASE WHEN f.is_refundable THEN 1 ELSE 0 END as is_refundable

FROM {{ ref('fact_flight_prices') }} f

-- JOIN DIMENSIONS
JOIN {{ ref('dim_date') }} d 
    ON f.date_key = d.date_key

JOIN {{ ref('dim_airlines') }} al 
    ON f.airline_key = al.airline_code

JOIN {{ ref('dim_airports') }} origin 
    ON f.origin_airport_key = origin.code

JOIN {{ ref('dim_airports') }} dest 
    ON f.dest_airport_key = dest.code

WHERE 
    f.total_fare > 0