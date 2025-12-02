{{
    config(
        materialized='incremental',
        unique_key='leg_id',
        on_schema_change='sync_all_columns',
        tags=['staging', 'flights']
    )
}}

with source as (
    select * from {{ source('raw', 'flights') }}
    {% if is_incremental() %}
    where SEARCHDATE >= (select max(search_date) from {{ this }})
    {% endif %}
),

parsed as (
    select
        -- Primary Key
        LEGID as leg_id,
        
        -- Dates (Type Casting)
        to_date(SEARCHDATE) as search_date,
        to_date(FLIGHTDATE) as flight_date,
        
        -- Airport Information
        upper(trim(STARTINGAIRPORT)) as starting_airport,
        upper(trim(DESTINATIONAIRPORT)) as destination_airport,
        
        -- Flight Characteristics (Booleans)
        coalesce(ISBASICECONOMY, false) as is_basic_economy,
        coalesce(ISREFUNDABLE, false) as is_refundable,
        coalesce(ISNONSTOP, false) as is_non_stop,
        
        -- Pricing (Type Casting)
        cast(BASEFARE as decimal(10,2)) as base_fare,
        cast(TOTALFARE as decimal(10,2)) as total_fare,
        
        -- Inventory & Distance
        cast(SEATSREMAINING as integer) as seats_remaining,
        cast(TOTALTRAVELDISTANCE as decimal(10,2)) as total_travel_distance,
        cast(ELAPSEDDAYS as integer) as elapsed_days,
        
        -- Duration Parsing
        case 
            when TRAVELDURATION like 'PT%H%M'
            then cast(regexp_substr(TRAVELDURATION, '[0-9]+', 1, 1) as integer) * 60 
                 + cast(regexp_substr(TRAVELDURATION, '[0-9]+', 1, 2) as integer)
            else null
        end as travel_duration_minutes,
        
        -- Segment Data
        SEGMENTSDEPARTURETIMEEPOCHSECONDS as segments_departure_epoch,
        SEGMENTSDEPARTURETIMERAW as segments_departure_raw,
        SEGMENTSARRIVALTIMEEPOCHSECONDS as segments_arrival_epoch,
        SEGMENTSARRIVALTIMERAW as segments_arrival_raw,
        SEGMENTSARRIVALAIRPORTCODE as segments_arrival_airports,
        SEGMENTSDEPARTUREAIRPORTCODE as segments_departure_airports,
        SEGMENTSAIRLINENAME as segments_airline_names,
        SEGMENTSAIRLINECODE as segments_airline_codes,
        SEGMENTSEQUIPMENTDESCRIPTION as segments_equipment,
        SEGMENTSDURATIONINSECONDS as segments_duration_seconds,
        SEGMENTSDISTANCE as segments_distances,
        SEGMENTSCABINCODE as segments_cabin_codes,
        
        -- Fare Basis Code
        FAREBASISCODE as fare_basis_code,
        
        -- Metadata
        current_timestamp() as loaded_at
        
    from source
),

stage_table as (
    select
        *,

        datediff(day, search_date, flight_date) as days_until_flight,
        
        -- Number of segments 
        array_size(split(segments_airline_codes, '||')) as num_segments,
        
        -- Extract first/last segment times
        split_part(segments_departure_epoch, '||', 1) as first_departure_epoch,
        split_part(segments_arrival_epoch, '||', -1) as last_arrival_epoch,
        
        -- Primary carrier
        split_part(segments_airline_codes, '||', 1) as primary_carrier,
        
        -- Primary cabin
        split_part(segments_cabin_codes, '||', 1) as primary_cabin,
        
        -- Pricing components
        cast(total_fare - base_fare as decimal(10,2)) as taxes_and_fees

    from parsed
)

select * from stage_table
where 
    leg_id is not null
    and search_date is not null
    and flight_date is not null
    and days_until_flight >= 0
    and days_until_flight <= 365
    and total_fare > 0
    and seats_remaining >= 0