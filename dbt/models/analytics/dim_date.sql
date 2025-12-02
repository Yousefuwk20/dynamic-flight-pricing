{{ config(
    materialized='table'
) }}

WITH date_spine AS (
    SELECT 
        DATEADD(day, SEQ4(), '2022-01-01') AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 1826))
),

enriched_dates AS (
    SELECT
        -- Primary Key
        TO_NUMBER(TO_CHAR(d.date_day, 'YYYYMMDD')) as date_key,
        
        -- Date Attributes
        d.date_day,
        YEAR(d.date_day) as year,
        MONTH(d.date_day) as month,
        MONTHNAME(d.date_day) as month_name,
        DAYOFWEEK(d.date_day) as day_of_week_num,
        DAYNAME(d.date_day) as day_name,
        -- Quarter attributes
        DATE_PART(quarter, d.date_day) as quarter,
        'Q' || DATE_PART(quarter, d.date_day) || ' ' || YEAR(d.date_day) as quarter_name,

        -- Weekend Flag
        CASE 
            WHEN DAYNAME(d.date_day) IN ('Sat', 'Sun') THEN TRUE 
            ELSE FALSE 
        END as is_weekend,
        
        -- Holiday Integration
        h.holiday_name,
        CASE 
            WHEN h.holiday_name IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END as is_holiday

    FROM date_spine d
    LEFT JOIN {{ ref('us_holidays') }} h
        ON d.date_day = h.holiday_date
)

SELECT * FROM enriched_dates
WHERE date_day <= '2027-01-01' 