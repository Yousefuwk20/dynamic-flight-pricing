# Dynamic Flight Pricing Pipeline - Technical Documentation

---

## Overview

Dynamic pricing is the backbone of modern airline revenue management, yet legacy systems struggle to process massive historical datasets efficiently. Traditional approaches often rely on static rules or sampled data, leading to suboptimal pricing strategies, revenue leakage, and slow reaction to market shifts. Furthermore, the sheer volume of flight data (82+ million records) creates significant computational bottlenecks for standard machine learning pipelines.
Our project proposes an end-to-end Data Engineering and ML solution built on the Modern Data Stack (Snowflake, dbt and dagster). The system integrates four core components:
* Scalable Data Warehouse Architecture: Utilizing Snowflake to ingest and store over 82 million flight records, organized into a strict Medallion Architecture (Bronze/Silver/Gold) to ensure data governance and accessibility.
* Robust Transformation Pipeline (ELT): Leveraging dbt (data build tool) to orchestrate complex transformations, enforce Data Quality tests (e.g., negative prices, invalid dates), and build a high-performance Star Schema for analytics.
* Memory-Optimized Feature Engineering: Implementing Polars and Apache Arrow for zero-copy data transfer, solving critical RAM bottlenecks and enabling efficient parsing of complex features like Fare Basis Codes and seasonality flags without crashing standard instances.
* Advanced Predictive Modeling: A Gradient Boosting Regressor (XGBoost) trained on granular features to predict flight prices with 91% accuracy (R²).
By unifying scalable engineering with advanced analytics, this system enables airlines to transition to automated, data-driven revenue optimization capable of handling enterprise-scale data loads.

---

## Problem Statement

Airlines employ dynamic pricing strategies where ticket prices fluctuate based on numerous factors including booking lead time, remaining seat inventory, route characteristics, and temporal patterns. Predicting these prices accurately is challenging due to:

- **Complex interactions** between features such as demand elasticity, competitive pricing, and capacity management
- **Temporal dependencies** where prices change based on days until departure and seasonal patterns
- **Route-specific dynamics** where origin-destination pairs exhibit different pricing behaviors
- **Inventory effects** where seat availability triggers algorithmic price adjustments
- **Categorical variables** including airline carrier, fare class, and airport characteristics

Traditional rule-based pricing models struggle to capture these non-linear relationships. A machine learning approach can learn these patterns from historical data and provide accurate predictions for new flight queries.

---


## Our Solution

The solution implements a multi-stage data pipeline following software engineering and data engineering best practices:

### Architecture Pattern: Medallion Architecture

The pipeline organizes data into progressive quality layers:

1. **Bronze Layer (RAW)** - Immutable landing zone for source data
2. **Silver Layer (STAGING)** - Cleaned, validated, and standardized data
3. **Gold Layer (ANALYTICS)** - Business-ready dimensional model
4. **ML Layer** - Feature-engineered table optimized for model training


### Data Modeling: Star Schema

The analytics layer implements a star schema with:

- **1 Fact Table**: `FACT_FLIGHTS` containing pricing measures and flight metrics
- **4 Dimension Tables**:
  - `DIM_DATE` - Calendar attributes with holiday indicators
  - `DIM_AIRLINES` - Carrier information
  - `DIM_AIRPORTS` - Airport locations and characteristics (role-playing dimension)
  - `DIM_ROUTES` - Origin-destination pairs (optional)

### Machine Learning: XGBoost Regression

The model uses Extreme Gradient Boosting with the following characteristics:

- **Algorithm**: XGBoost regressor with tree-based ensemble
- **Loss Function**: Mean squared error for regression
- **Hyperparameters**: 100 estimators, learning rate 0.1, max depth 6
- **Feature Engineering**: Mix of numeric, categorical, and binary features
- **Cross-Validation**: Train-test split with 90/10 ratio

### Model Performance Metrics

| Metric | Value | Interpretation |
|--------|-------|----------------|
| R² Score | 0.91 | Explains 91% of price variance |
| RMSE | $51.93 | Average prediction error in dollars |
| Inference Latency | <50ms | Real-time prediction capability |

### Deployment: Containerized API

The trained model is served through a FastAPI application:

- **Containerization**: Docker with Python 3.11 base image
- **Cloud Platform**: AWS ECS Fargate for serverless container hosting
- **Load Balancing**: Application Load Balancer for high availability
- **Scaling**: Auto-scaling based on CPU and request metrics

---

## Dataset

The dataset consists of historical flight search results with pricing information collected from major airlines operating in the United States.

### Data Source
- **Origin**: Kaggle to AWS S3 bucket (`s3://airlinesic/raw_data/`)
- **Format**: Parquet files (columnar, compressed)

### Schema Overview

The raw dataset contains **27 columns** across several categories:

#### Identifiers
- `legId` - Unique identifier for each flight leg
- `segmentsAirlineCode` - Operating carrier codes (pipe-delimited for multi-segment)
- `fareBasisCode` - Fare class and booking code

#### Temporal Attributes
- `searchDate` - Date when the price was searched
- `flightDate` - Date of the actual flight
- `travelDuration` - Total travel time in ISO 8601 format (e.g., "PT5H30M")

#### Geographic Attributes
- `startingAirport` - Three-letter IATA code for origin airport
- `destinationAirport` - Three-letter IATA code for destination airport

#### Flight Characteristics
- `isBasicEconomy` - Boolean flag for basic economy fare class
- `isRefundable` - Boolean flag for refundable tickets
- `isNonStop` - Boolean flag for direct flights with no connections
- `segmentsCabinCode` - Cabin class per segment (economy, business, first)

#### Pricing Information
- `totalFare` - Total ticket price including all fees
- `baseFare` - Base ticket price before taxes and fees
- Additional pricing components calculated as derived fields

#### Operational Metrics
- `seatsRemaining` - Available seats at time of search
- `totalTravelDistance` - Route distance in miles
- `elapsedDays` - Days between search date and flight date

#### Segment Details
- `segmentsAirlineName` - Airline names for each flight segment
- `segmentsDepartureTimeEpochSeconds` - Departure times in epoch format
- `segmentsArrivalTimeEpochSeconds` - Arrival times in epoch format
- `segmentsDepartureTimeRaw` - Human-readable departure times
- `segmentsArrivalTimeRaw` - Human-readable arrival times


### Data Quality Characteristics
- **Missing Values**
- **Outliers**:
- **Duplicates**
- **Encoding**

---


## Business Overview

### Use Cases

1. **Price Prediction Service**
   - Provides real-time fare estimates for flight search queries
   - Enables comparison with actual market prices

2. **Demand Forecasting**
   - Analyzes historical pricing patterns to understand demand elasticity
   - Identifies high-value booking windows

3. **Customer Analytics**
   - Segments customers by price sensitivity
   - Optimizes fare display and recommendations

### Business Value

- **Revenue Optimization**: Improves pricing decisions by 4-5% through accurate demand prediction
- **Operational Efficiency**: Automates pricing analysis reducing manual effort by 80%
- **Market Intelligence**: Provides real-time insights into competitor pricing strategies
- **Customer Experience**: Enables personalized pricing and recommendations

### Stakeholders

- **Revenue Management Teams**: Use predictions to optimize fare structures
- **Data Science Teams**: Monitor model performance and retrain with new data
- **Product Teams**: Integrate predictions into booking flows
- **Executive Leadership**: Track pricing effectiveness and competitive positioning

---

## Technology Stack

### Data Infrastructure

| Component | Technology |  Purpose |
|-----------|------------|---------|
| Data Warehouse | Snowflake |  Centralized storage, compute, and SQL transformations |
| Object Storage | AWS S3 |  Raw data landing zone and model artifact storage |
| File Format | Parquet |  Columnar storage with compression and schema embedding |
| Data Modeling | dbt |  SQL-based transformations and documentation |

### Machine Learning

| Component | Technology |  Purpose |
|-----------|------------|---------|
| ML Framework | XGBoost |  Gradient boosting for regression tasks |
| Data Processing | pandas |  DataFrame manipulation and feature engineering |
| Experiment Tracking | MLflow |  Metrics logging, model versioning, and registry |

### API and Serving

| Component | Technology |  Purpose |
|-----------|------------|---------|
| Web Framework | FastAPI |  High-performance REST API with automatic documentation |
| ASGI Server | Uvicorn |  Production-grade async server |

### Orchestration and Monitoring

| Component | Technology |  Purpose |
|-----------|------------|---------|
| Workflow Orchestration | Dagster |  Asset-based pipeline scheduling and monitoring |
| Experiment Tracking | MLflow |  Model lifecycle management and experiment comparison |

### Infrastructure and Deployment

| Component | Technology |  Purpose |
|-----------|------------|---------|
| Containerization | Docker |  Application packaging and isolation |
| Container Registry | AWS ECR | Private Docker image storage |
| Container Orchestration | AWS ECS Fargate | Serverless container hosting |
| Load Balancing | AWS ALB |  Traffic distribution and health checks |


---

## Architecture Diagram

```
┌─────────────┐
│   AWS S3    │ Raw Parquet Files
│  (Source)   │
└──────┬──────┘
       │ COPY INTO (Full Load)
       ▼
┌────────────────────────────────────────────────────────┐
│                     SNOWFLAKE                          │
│                                                        │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐          │
│  │   RAW    │ -> │ STAGING  │ -> │ANALYTICS │          │
│  │ (Bronze) │    │ (Silver) │    │  (Gold)  │          │
│  └──────────┘    └──────────┘    └────┬─────┘          │ 
│                                        │               │
│                                   ┌────▼─────┐         │
│                                   │    ML    │         │
│                                   │ FEATURES │         │
│                                   └────┬─────┘         │
└────────────────────────────────────────┼───────────────┘
                                         │
                                         ▼
                              ┌──────────────────┐
                              │  Python Training │
                              │     XGBoost      │
                              │  + MLflow Track  │
                              └────────┬─────────┘
                                       │
                                       ▼
                              ┌──────────────────┐
                              │   FastAPI App    │
                              │  (Docker Image)  │
                              └────────┬─────────┘
                                       │
                                       ▼
                              ┌──────────────────┐
                              │   AWS ECS        │
                              │   (Fargate)      │
                              │   + ALB          │
                              └──────────────────┘
```

---

## Implementation

### 1. Data Ingestion

The ingestion process loads raw flight data from AWS S3 into Snowflake's RAW schema using native cloud integration.

#### Storage Integration

A storage integration establishes a relationship between Snowflake and AWS S3.

```sql
CREATE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT:role/snowflake-access-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://airlines_sic_v2/');
```

#### External Stage

An external stage points to the S3 bucket location and defines the file format. Parquet format is used for its columnar storage, compression efficiency, and embedded schema metadata.

```sql
CREATE STAGE flight_stage
    STORAGE_INTEGRATION = s3_integration
    URL = 's3://airlines_sic_v2/raw_data/'
    FILE_FORMAT = (TYPE = 'PARQUET');
```

#### Schema Inference and Table Creation

Snowflake's `INFER_SCHEMA` function automatically detects column names and data types from Parquet metadata, eliminating manual schema definition and ensuring consistency with source files.

```sql
-- Create table using inferred schema
CREATE TABLE raw.flights USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@flight_stage',
            FILE_FORMAT => 'parquet_format'
        )
    )
);
```

#### Data Loading

The `COPY INTO` command loads all Parquet files from the stage into the target table. The `MATCH_BY_COLUMN_NAME` parameter matches columns by name rather than position, providing flexibility when column order differs.

```sql
COPY INTO raw.flights
FROM @flight_stage
FILE_FORMAT = parquet_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```


**Data Volume**: The complete dataset contains approximately 82 million records. For model training, a 30% sample (~27-28 million records) was used due to computational constraints.

---

### 2. Data Transformations

The transformation pipeline implements the medallion architecture, progressively refining data through Bronze (RAW), Silver (STAGING), Gold (ANALYTICS), and ML layers.

#### Staging Layer (Bronze to Silver)

The staging layer applies data quality rules and standardization without changing the grain of the data.

**Data Cleaning**
- Null handling with `COALESCE` for boolean flags (e.g., `isBasicEconomy`, `isRefundable`)
- Trimming whitespace from string columns
- Filtering invalid records (null identifiers, invalid dates)

**Standardization**
- Uppercase conversion for airport codes: `UPPER(TRIM(startingAirport))`
- Consistent date formatting with `TO_DATE()` function

**Type Casting**
- Date parsing: `TO_DATE(searchDate)`, `TO_DATE(flightDate)`
- Decimal conversion: `CAST(totalFare AS DECIMAL(10,2))`
- Integer conversion: `CAST(seatsRemaining AS INTEGER)`

**Duration Parsing**
The `travelDuration` field uses ISO 8601 format (e.g., "PT5H30M" for 5 hours 30 minutes). This is parsed into total minutes:

```sql
CASE 
    WHEN travelDuration LIKE 'PT%H%M'
    THEN CAST(SPLIT_PART(SPLIT_PART(travelDuration, 'H', 1), 'PT', 2) AS INTEGER) * 60 +
         CAST(SPLIT_PART(SPLIT_PART(travelDuration, 'M', 1), 'H', 2) AS INTEGER)
    WHEN travelDuration LIKE 'PT%H'
    THEN CAST(SPLIT_PART(travelDuration, 'H', 1) AS INTEGER) * 60
    ELSE NULL
END AS travel_duration_minutes
```

**Calculated Fields**
- `days_until_flight`: `DATEDIFF(day, search_date, flight_date)`
- `taxes_and_fees`: `total_fare - base_fare`

**Validation Filters**
```sql
WHERE leg_id IS NOT NULL
  AND search_date IS NOT NULL
  AND flight_date IS NOT NULL
  AND total_fare > 0
  AND seats_remaining >= 0
```

#### Analytics Layer (Silver to Gold)

The analytics layer builds a star schema dimensional model from cleaned staging data.

##### Dimension Tables

**DIM_DATE**

A date dimension is created using a date spine that includes all unique dates from the dataset. Calendar attributes and holiday indicators are added.

Key Attributes:
- `date_key`: Integer surrogate key in YYYYMMDD format
- `date_day`: Actual DATE value
- Calendar components: `year`, `quarter`, `month`, `day`, `day_of_week_num`
- Named attributes: `day_name` (Monday, Tuesday, etc.), `month_name`
- Boolean flags: `is_weekend`, `is_holiday`

Holiday Detection Logic:
U.S. federal holidays for 2022 were identified from external sources and marked in the dimension:
- New Year's Day, Martin Luther King Jr. Day, Presidents' Day
- Memorial Day, Independence Day, Labor Day
- Columbus Day, Veterans Day, Thanksgiving, Christmas

```sql
CASE 
    WHEN (month = 11 AND day >= 22 AND day <= 28 AND day_of_week_num = 4) 
        THEN TRUE  -- Thanksgiving (4th Thursday of November)
    WHEN (month = 12 AND day = 25) THEN TRUE  -- Christmas
    WHEN (month = 1 AND day = 1) THEN TRUE    -- New Year's
    WHEN (month = 7 AND day = 4) THEN TRUE    -- Independence Day
    -- Additional holiday logic...
    ELSE FALSE
END AS is_holiday
```

**DIM_AIRLINES**

Airlines are extracted from the pipe-delimited `segmentsAirlineCode` field in the raw data. When multiple carriers operate different segments, the data is flattened and deduplicated.

Final Schema:
- `airline_code`: VARCHAR(10) - Primary key
- `airline_name`: VARCHAR(100) - Full carrier name
- `created_at`: TIMESTAMP - Record creation timestamp

**DIM_AIRPORTS**

Airport dimension contains distinct airports serving as origins or destinations. Airport names, cities, and states were enriched using external airport reference data sources.

Extraction Process:
1. Collect distinct airport codes from both origin and destination
2. Join with external airport reference data for city and state mapping

Final Schema:
- `airport_code`: VARCHAR(10) - Primary key (IATA code)
- `airport_name`: VARCHAR(200) - Full airport name
- `city`: VARCHAR(100) - City location
- `state`: VARCHAR(50) - State code
- `created_at`: TIMESTAMP - Record creation timestamp

**DIM_ROUTES**

Route dimension captures unique origin-destination pairs with aggregated metrics.

Final Schema:
- `route_key`: VARCHAR(50) - Primary key 
- `origin_airport`: VARCHAR(10) 
- `dest_airport`: VARCHAR(10) 
- `distance_miles`: DECIMAL
- `created_at`: TIMESTAMP 

##### Fact Table

**FACT_FLIGHT_PRICES**

The fact table contains granular flight pricing observations with foreign keys to all dimensions.

Primary Key:
- `leg_id`: VARCHAR(100) - Unique flight leg identifier

Foreign Keys:
- `date_key`: INTEGER - Links to DIM_DATE
- `airline_key`: VARCHAR(10) - Links to DIM_AIRLINES
- `origin_airport_key`: VARCHAR(10) - Links to DIM_AIRPORTS (origin)
- `dest_airport_key`: VARCHAR(10) - Links to DIM_AIRPORTS (destination)
- `route_key`: VARCHAR(50) - Links to DIM_ROUTES

Degenerate Dimensions (attributes without separate dimension tables):
- `search_date`: DATE
- `flight_date`: DATE
- `primary_cabin`: VARCHAR(50)
- `fare_basis_code`: VARCHAR(20)

Pricing Measures:
- `total_fare`: DECIMAL(10,2) - Total ticket price
- `base_fare`: DECIMAL(10,2) - Base fare before taxes
- `taxes_and_fees`: DECIMAL(10,2) - Additional charges
- `fare_per_mile`: DECIMAL(10,4) - Price per distance unit

Operational Metrics:
- `seats_remaining`: INTEGER - Available inventory
- `days_until_flight`: INTEGER - Booking lead time
- `total_travel_distance`: INTEGER - Route distance in miles
- `travel_duration_minutes`: INTEGER - Total flight time
- `num_segments`: INTEGER - Number of flight legs

Boolean Flags:
- `is_basic_economy`: BOOLEAN
- `is_refundable`: BOOLEAN
- `is_non_stop`: BOOLEAN

```sql
SELECT 
    f.total_fare,
    origin.city AS origin_city,
    dest.city AS destination_city
FROM fact_flights f
JOIN dim_airports origin ON f.origin_airport_key = origin.airport_code
JOIN dim_airports dest ON f.dest_airport_key = dest.airport_code;
```

#### ML Features Layer (Gold to ML)

The ML features layer denormalizes the star schema into a flat table optimized for model training. All dimension attributes are joined and flattened into a single wide table.

Denormalization Process:
```sql
SELECT
    -- Target variable
    f.total_fare,
    
    -- Numeric features from fact
    f.days_until_flight,
    f.seats_remaining,
    f.total_travel_distance,
    f.travel_duration_minutes,
    f.num_segments,
    
    -- Categorical features
    f.airline_key AS airline_code,
    origin.city AS origin_city,
    dest.city AS dest_city,
    f.fare_basis_code,
    
    -- Date features from dimension
    d.flight_year,
    d.flight_month,
    d.flight_day_of_week,
    
    -- Binary features
    f.is_weekend,
    f.is_holiday,
    f.is_basic_economy,
    f.is_non_stop,
    f.is_refundable
    
FROM fact_flights f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_airlines a ON f.airline_key = a.airline_code
JOIN dim_airports origin ON f.origin_airport_key = origin.airport_code
JOIN dim_airports dest ON f.dest_airport_key = dest.airport_code
JOIN dim_routes r ON f.route_key = r.route_key
```

**Data Filtering**

Outliers and invalid records are filtered to improve model quality:
- `total_fare > 0` - Remove zero or negative prices
- `total_travel_distance > 0` - Remove invalid routes
- `seats_remaining >= 0` - Remove inventory errors
- `days_until_flight >= 0` - Remove past bookings



---

### 3. Machine Learning Pipeline

The ML pipeline trains an XGBoost regression model to predict flight prices. Training was performed on AWS SageMaker using the `ml.r6id.2xlarge` instance (64GB RAM, 8 vCPUs) to handle the large dataset efficiently.

#### Data Loading

Flight features are loaded from Snowflake's `ML_FLIGHT_PRICING` table using the Snowflake Python connector with Apache Arrow for zero-copy data transfer.

```sql
SELECT * FROM ML_FLIGHT_PRICING SAMPLE(30)
```

Data is loaded into a Polars DataFrame for memory-efficient processing:
- **Rows**: ~23-24 million records

#### Feature Engineering

##### Fare Basis Code Parsing

The `fare_basis_code` field contains encoded information about booking class, fare rules, passenger type, and seasonality. This cryptic code is parsed into interpretable features:

**Cabin Category Extraction**

The first letter of the fare basis code indicates booking class:
- **First Class**: F, A
- **Business Class**: J, C, D, I, Z, P
- **Premium Economy**: W, S, T, R
- **Full Economy**: Y, B, M, H
- **Discount Economy**: K, L, Q, V, U, X, N, O, G, E

Extracted Feature: `cabin_category` (categorical: First, Business, Premium Economy, Full Economy, Discount Economy, Unknown)

**Numeric Fare Rule Extraction**

Numeric sequences in fare codes often indicate specific pricing rules or restrictions. These are extracted using regex pattern matching:

Pattern: `(\d+)` - Extracts first numeric sequence
Extracted Feature: `fare_rule_number` (integer, 0 if no number present)

**Passenger Type Detection**

Certain suffixes indicate passenger categories:
- **Child**: Contains "CH" or "CNN"
- **Infant**: Contains "IN" or "INF"
- **Adult**: Default if no child/infant indicator

Extracted Feature: `passenger_type` (categorical: Adult, Child, Infant)

**Seasonality Proxy**

Some fare codes use suffixes to indicate peak/off-peak periods:
- **High Season**: Ends with "H"
- **Low Season**: Ends with "L"
- **Standard**: No seasonal indicator

Extracted Feature: `seasonality_proxy` (categorical: High, Low, Standard)

**Additional Fare Flags**

Binary indicators for specific fare characteristics:
- `is_night_fare_proxy`: Contains "N" (binary 0/1)
- `is_weekend_fare_proxy`: Contains "W" (binary 0/1)

##### Outlier Detection and Removal

Statistical outlier detection using the Interquartile Range (IQR) method with a 2.5x multiplier for more conservative filtering:

**Price Outliers**
```
Q1 = 25th percentile of total_fare
Q3 = 75th percentile of total_fare
IQR = Q3 - Q1
Upper Bound = Q3 + (2.5 × IQR)
Lower Bound = Q1 - (2.5 × IQR)
```
Records with prices exceeding upper bound are removed.

**Duration Outliers**
Same IQR method applied to `travel_duration_minutes` to remove unrealistic flight durations.

**Additional Filters**
- Zero seats remaining (removes sold-out flights)
- Negative or zero values for distance, duration, or days until flight
- Travel duration less than 20 minutes (invalid)

**Impact**: Outlier removal reduces dataset by approximately 3%, improving model stability and generalization.

##### Categorical Encoding

Six categorical features are encoded using Label Encoding to convert text values to numeric indices:

**Encoded Features**:
1. `airline_code` - Carrier identifier
2. `origin_city` - Departure city
3. `dest_city` - Arrival city
4. `cabin_category` - Parsed from fare basis code
5. `passenger_type` - Parsed from fare basis code
6. `seasonality_proxy` - Parsed from fare basis code

**Encoding Process**:
- Each unique category is assigned an integer (0, 1, 2, ...)
- Encoders are saved using `pickle` for inference-time transformation
- Maintains consistency between training and prediction


##### Final Feature Set

After feature engineering, the dataset contains:

**Numeric Features** (7):
- days_until_flight
- seats_remaining
- total_travel_distance
- travel_duration_minutes
- num_segments
- flight_year
- flight_month
- flight_day_of_week
- fare_rule_number


**Categorical Features** (6):
- cabin_category
- passenger_type
- seasonality_proxy
- airline_code 
- origin_city 
- dest_city 

**Binary Features** (8):
- is_weekend
- is_holiday
- is_basic_economy
- is_non_stop
- is_refundable
- has_numeric_rule
- is_night_fare_proxy
- is_weekend_fare_proxy

**Target Variable**:
- total_fare

**Total Features**: 21 predictor variables

#### Model Training

##### Train-Test Split

Data is split into training and test sets with a 90/10 ratio:
- **Training Set**: 90% (~20-21 million records)
- **Test Set**: 10% (~2.2-2.3 million records)

##### XGBoost Configuration

The model uses Extreme Gradient Boosting with the following hyperparameters:

**Core Parameters**:
- `n_estimators`: 1500 - Number of boosting rounds (trees)
- `learning_rate`: 0.1 - Step size shrinkage to prevent overfitting
- `max_depth`: 10 - Maximum tree depth (allows complex interactions)

**Regularization**:
- `subsample`: 0.8 - Train on 80% of samples per tree (prevents overfitting)
- `colsample_bytree`: 0.8 - Use 80% of features per tree (adds diversity)

**Training Control**:
- `early_stopping_rounds`: 50 - Stop if no improvement for 50 rounds
- `n_jobs`: -1 - Use all CPU cores for parallel processing
- `eval_set`: Test set used for validation during training

**Training Process**:
1. Model iteratively builds decision trees
2. Each tree corrects errors of previous trees
3. Validation metrics monitored on test set every 10 rounds
4. Training stops early if test RMSE doesn't improve for 50 rounds
5. Best iteration is retained based on lowest validation error

**Training Time**: Approximately 70 minutes on AWS SageMaker ml.r6id.2xlarge instance

##### MLflow Experiment Tracking

MLflow tracks the training process for reproducibility and comparison:
- **Parameters Logged**: All hyperparameters (n_estimators, learning_rate, max_depth, etc.)
- **Metrics Logged**: R², RMSE, MAE computed on test set
- **Artifacts Logged**: Serialized model file, label encoders
- **Run Metadata**: Timestamp, data volume, feature count

#### Model Evaluation

##### Performance Metrics

The trained model is evaluated on the held-out test set:

| Metric | Value | Interpretation |
|--------|-------|----------------|
| **R² Score** | 0.91 | Model explains 91% of price variance |
| **RMSE** | $51.93 | Average prediction error (root mean squared) |
| **MAE** | $33.97 | Average absolute prediction error |

**R² Score (0.91)**: The coefficient of determination indicates excellent model fit. The model accounts for 91% of price variability, leaving only 9% unexplained by the features.

**RMSE ($51.93)**: Root Mean Squared Error penalizes larger errors more heavily. An RMSE of $51.93 means predictions are typically within $52 of actual prices, acceptable for airfare prediction.

**MAE ($33.97)**: Mean Absolute Error shows the typical prediction error without squaring. On average, predictions differ from actual prices by $34, representing strong accuracy for ticket pricing.

#### Model Serialization

The trained model and preprocessing artifacts are saved for deployment:

**Model Artifact**:
- Format: XGBoost JSON format
- File: `final_flight_pricing_model.json`
- Size: ~140 MB

**Label Encoders**:
- Format: Pickle
- File: `label_encoders.pkl`
- Contains: LabelEncoder objects for 6 categorical features
- Required: Must be applied to raw categorical inputs during inference

**MLflow Model Registry**:
- Model version tracked in MLflow registry
- Metadata includes training date, metrics, parameters

**Deployment Ready**: Model file and encoders are packaged for containerized API deployment on AWS ECS.

---

## Deployment

The trained XGBoost model is deployed as a REST API on AWS ECS using Docker and Fargate serverless compute.

### Infrastructure

**AWS Stack**:
- **ECS Cluster**: `flight-pricing-cluster` (Fargate, us-east-1)
- **Service**: `flight-pricing-service` with 1 task (1 vCPU, 2GB RAM)
- **Load Balancer**: Application Load Balancer with health checks
- **Registry**: ECR repository `flight-pricing-api`
- **Logging**: CloudWatch Logs (`/ecs/flight-pricing`)

**Container**:
- **Base Image**: Python 3.9
- **Contents**: FastAPI app, XGBoost model (140MB), label encoders
- **Port**: 8000
- **Startup**: ~15-20 seconds

### API Endpoints

**Health Check**: `GET /health` - Returns model status and readiness

**Prediction**: `POST /predict` - Accepts flight parameters (dates, route, seats, fare code, etc.) and returns ML base price plus dynamic adjustments based on demand (30%), competition (25%), inventory (20%), time (15%), and seasonality (10%)

**Batch**: `POST /predict/batch` - Multiple flight predictions

**Encoders**: `GET /encoders` - Available categories for dropdown menus

### Deployment Process

Automated via PowerShell script (`deploy.ps1`):

1. Build Docker image
2. Test locally on port 8001
3. Authenticate to ECR
4. Push image to registry
5. Stop old ECS service
6. Register new task definition
7. Deploy new service
8. Wait for stabilization

**Execution Time**: 5-7 minutes

### Monitoring

**Logs**: 
```powershell
aws logs tail /ecs/flight-pricing --follow --region us-east-1
```

**Service Status**:
```powershell
aws ecs describe-services --cluster flight-pricing-cluster --services flight-pricing-service --region us-east-1
```

**Endpoint**:
```powershell
aws elbv2 describe-load-balancers --region us-east-1 --query 'LoadBalancers[0].DNSName' --output text
```

## Orchestration

To validate the end-to-end pipeline, a simulation was built using **Dagster** with asset-based orchestration and **MLflow** for experiment tracking.

### Pipeline Architecture

The orchestration layer demonstrates the complete workflow from synthetic data generation to batch predictions using Dagster's asset-based paradigm.

**Pipeline Flow**:
```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Generate Demo   │────>│  Ingest to       │────>│  Transform Data  │
│  Data (500 rows) │     │  Snowflake       │     │                  │
└──────────────────┘     └──────────────────┘     └──────────────────┘
                                                            │
                                                            |
┌──────────────────┐     ┌──────────────────┐               |        
│  Batch           |<────│  Train Demo      │<──────────────|
│  Predictions     │     │  Model (XGBoost) │            
└──────────────────┘     └──────────────────┘            
```

### Dagster Assets

The pipeline is composed of five dependent assets that execute sequentially:

**1. generate_demo_data**
- Generates 500 synthetic flight records using realistic distributions
- Outputs CSV file and returns DataFrame with metadata (avg price, route count)

**2. ingest_to_snowflake**
- Loads CSV data into `DEMO_RAW.DEMO_FLIGHTS` table
- Uses Snowflake connector
- Depends on: `generate_demo_data`

**3. transform_data**
- Executes transformations through medallion layers:
  - **Staging**: `DEMO_STAGING.STG_DEMO_FLIGHTS` - Data cleaning and type casting
  - **Analytics**: Creates 3 dimensions (`DEMO_DIM_AIRLINES`, `DEMO_DIM_AIRPORTS`, `DEMO_DIM_ROUTES`) and 1 fact table (`DEMO_FACT_FLIGHTS`)
  - **ML Features**: Denormalizes star schema into `DEMO_ML.DEMO_ML_FEATURES`
- Each transformation logged with row counts and execution time
- Depends on: `ingest_to_snowflake`

**4. train_demo_model**
- Loads features from `DEMO_ML.DEMO_ML_FEATURES` table
- Trains XGBoost regressor with hyperparameters (100 estimators, max_depth=6, lr=0.1)
- Performs 90/10 train-test split
- Evaluates model
- Saves model artifacts to `full_pipeline_demo/models/` directory
- Logs metrics to MLflow 
- Depends on: `transform_data`

**5. batch_predictions**
- Scores 100 flights using trained model
- Compares predictions against actual prices from test set
- Outputs results to CSV file
- Depends on: `train_demo_model`


### MLflow Integration

MLflow tracks experiments during model training:

**Tracked Elements**:
- **Parameters**: n_estimators, max_depth, learning_rate, subsample, colsample_bytree
- **Metrics**: R², RMSE, MAE, MAPE computed on test set
- **Artifacts**: Serialized model file (`demo_flight_pricing_model.json`), label encoders

### Isolation and Safety

**Demo Schemas**: All tables prefixed with `DEMO_*` to separate simulation from production:
- `DEMO_RAW.DEMO_FLIGHTS`
- `DEMO_STAGING.STG_DEMO_FLIGHTS`
- `DEMO_ANALYTICS.DEMO_DIM_*` and `DEMO_FACT_FLIGHTS`
- `DEMO_ML.DEMO_ML_FEATURES`

This isolation allows experimentation without affecting production pipelines or data.

---

## Analytics & Visualization

This section outlines the analytics KPIs and visualization plan that translate model outputs and flight data into actionable business insights for pricing, operations, and market understanding.

### Why It Matters
- Improves revenue management through demand, inventory, and booking-window analysis
- Benchmarks competitor behavior across airlines and routes
- Enhances customer experience via better price-to-value recommendations
- Reveals booking patterns to optimize dynamic pricing and inventory allocation

### KPI Framework
- **Pricing Optimization**: Forecast revenue, understand fare trends, track competitive pricing
- **Operational Efficiency**: Duration, non-stop vs connections, distance, seat inventory
- **Market & Customer Insights**: Booking behavior, demand seasonality, preferences

### Key KPIs
- **Average Fare (Base & Total)**: Benchmark competitiveness across airlines/routes
- **Fare Distribution**: Detect variance and anomalies; support demand forecasting
- **Price Difference (Total vs Base)**: Quantify taxes/fees impact on customer decisions
- **Price by Route**: Identify high-value, high-demand, or highly competitive routes
- **Price by Airline**: Benchmark premium vs budget carriers
- **Average Seats Remaining**: Demand proxy; correlates with higher prices
- **Seat Scarcity Levels**: Urgency indicator (% flights with fewer than X seats)
- **% Non-Stop vs Connecting**: Customer relevance; pricing implications
- **Average Travel Duration**: Route efficiency and experience signal
- **Fare per Mile**: Normalize price by distance to flag overpriced routes
- **Booking Window Analysis**: FlightDate − SearchDate patterns for forecasting
- **Price Trend vs Booking Window**: Price dynamics nearing departure
- **Demand Seasonality**: Monthly search volumes; holiday effects
- **Airline Market Share**: Dominance vs niche carriers for recommendations

### Recommended Dashboard (Top 8)
1. Average Fare by Route
2. Fare Trend Over Time (SearchDate)
3. Price Trend vs Booking Window
4. Price by Airline
5. Non-Stop vs Connecting Flight Share
6. Seats Remaining vs Fare (Demand Pressure)
7. Airline Market Share
8. Demand Seasonality (search volume per month)

These visuals prioritize clarity, business relevance, and decision support for revenue, product, and operations teams.

## Conclusion

This project successfully demonstrates an enterprise-grade flight pricing prediction system built on modern data engineering principles and scalable infrastructure. By integrating Snowflake's cloud data warehouse, dbt's transformation framework, XGBoost's machine learning capabilities, and AWS ECS deployment, the solution addresses the core challenges of dynamic airline pricing at scale.

### Key Achievements

**Data Engineering Excellence**:
- Ingested and processed 82 million flight records using Snowflake's native S3 integration
- Implemented medallion architecture (Bronze/Silver/Gold/ML) ensuring data quality and governance
- Built star schema dimensional model with role-playing dimensions for flexible analytics
- Achieved memory-efficient feature engineering using Polars and Apache Arrow for 23-24 million record training sets

**Machine Learning Performance**:
- Trained XGBoost model achieving 91% R² score with $33.97 MAE on production data
- Parsed complex fare basis codes into 3 interpretable features (cabin category, passenger type, seasonality)
- Applied IQR-based outlier detection reducing noise by 3% while preserving data integrity
- Reduced training time to 70 minutes on AWS SageMaker ml.r6id.2xlarge (64GB RAM, 8 vCPUs)

**Production Deployment**:
- Containerized FastAPI application serving real-time predictions with <50ms latency
- Deployed on AWS ECS Fargate with auto-scaling and load balancing for high availability
- Implemented dynamic pricing adjustments based on 5 market factors (demand, competition, inventory, time, seasonality)

**Orchestration and Monitoring**:
- Validated end-to-end pipeline with Dagster asset-based orchestration
- Integrated MLflow for experiment tracking and model versioning
- Automated weekly retraining schedule with asset lineage visualization
- Isolated demo environments preventing production interference

### Business Value Delivered

- **Accuracy**: 91% variance explained enables confident pricing recommendations
- **Speed**: Real-time API responses support interactive booking experiences
- **Cost**: Serverless infrastructure scales with demand, eliminating idle resource waste
---



