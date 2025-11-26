{{
    config(
        materialized='table',
        tags=['dimensions', 'airlines']
    )
}}

with source_data as (
    select 
        segments_airline_codes as raw_codes,
        segments_airline_names as raw_names
    from {{ ref('staging_model') }}
    where segments_airline_codes is not null
),

flattened_airlines as (
    select
        TRIM(f.value::STRING) as airline_code,
        TRIM(SPLIT(t.raw_names, '||')[f.index]::STRING) as airline_name

    from source_data t,
    LATERAL FLATTEN(input => SPLIT(t.raw_codes, '||')) f
    
    where airline_code is not null 
      and airline_code != ''
),

ranked_airlines as (

    select 
        airline_code,
        airline_name,
        count(*) as frequency,
        row_number() over (partition by airline_code order by count(*) desc) as rn
    from flattened_airlines
    where airline_name is not null 
      and airline_name != ''
    group by airline_code, airline_name
)

select
    airline_code,
    airline_name,
    current_timestamp() as created_at
from ranked_airlines
where rn = 1
order by airline_code