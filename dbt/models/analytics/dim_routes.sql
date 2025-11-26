{{
    config(
        materialized='table',
        tags=['dimensions', 'routes']
    )
}}


with flight_stats as (
    select 
        starting_airport as origin_airport,
        destination_airport as dest_airport,
        AVG(total_travel_distance)::INT as distance_miles,
        count(*) as num_flights_observed
    from {{ ref('staging_model') }} 
    where starting_airport is not null 
      and destination_airport is not null
    group by 1, 2
)


select
    origin_airport || '_' || dest_airport as route_key,
    origin_airport,
    dest_airport,
    COALESCE(distance_miles, 0) as distance_miles,
    current_timestamp() as created_at
from flight_stats