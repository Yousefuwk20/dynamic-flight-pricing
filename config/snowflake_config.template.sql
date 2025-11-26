-- =============================================================================
-- Snowflake Configuration Template
-- =============================================================================
-- Replace placeholders with your actual values:
--   <SNOWFLAKE_ACCOUNT>  - Your Snowflake account identifier
--   <AWS_ACCOUNT_ID>     - Your AWS account ID
--   <S3_BUCKET_NAME>     - Your S3 bucket name
--   <IAM_ROLE_NAME>      - Your IAM role for Snowflake access
-- =============================================================================

CREATE DATABASE IF NOT EXISTS flight_pricing;

CREATE SCHEMA IF NOT EXISTS flight_pricing.raw;          
CREATE SCHEMA IF NOT EXISTS flight_pricing.staging;      
CREATE SCHEMA IF NOT EXISTS flight_pricing.analytics;    
CREATE SCHEMA IF NOT EXISTS flight_pricing.ml;           

CREATE WAREHOUSE IF NOT EXISTS flight_wh
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60       
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE flight_wh;
USE DATABASE flight_pricing;
USE SCHEMA raw;


CREATE OR REPLACE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/<IAM_ROLE_NAME>'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<S3_BUCKET_NAME>/');

DESC INTEGRATION s3_integration;


CREATE OR REPLACE FILE FORMAT parquet_format
    TYPE = 'PARQUET';

CREATE OR REPLACE STAGE flight_stage
    STORAGE_INTEGRATION = s3_integration
    URL = 's3://<S3_BUCKET_NAME>/raw_data/'
    FILE_FORMAT = parquet_format;

LIST @flight_stage;


SELECT *
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@flight_stage',
        FILE_FORMAT => 'parquet_format'
    )
);

CREATE OR REPLACE TABLE raw.flights
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@flight_stage',
            FILE_FORMAT => 'parquet_format'
        )
    )
);


COPY INTO raw.flights
FROM @flight_stage
FILE_FORMAT = parquet_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT COUNT(*) FROM raw.flights;
SELECT * FROM raw.flights LIMIT 10;
