# Flight Pricing API - Deployment Guide

## Overview
This API uses XGBoost with 23 features for flight price prediction, plus dynamic pricing adjustments based on demand, competition, inventory, time, and seasonality factors.

## Files
- `api.py` - FastAPI application
- `pricing_system.py` - ML prediction and dynamic pricing logic
- `index.html` - Web interface
- `flight_pricing_model.json` - XGBoost model (23 features)
- `label_encoders.pkl` - Label encoders for categorical features
- `dockerfile` - Docker container definition
- `requirements.txt` - Python dependencies
- `task-definition.json` - AWS ECS task definition
- `service-definition.json` - AWS ECS service configuration
- `deploy.ps1` - Automated deployment script
- `rollback.ps1` - Rollback to previous version

## Prerequisites
1. AWS CLI configured with proper credentials
2. Docker Desktop installed and running
3. PowerShell (for deployment scripts)
4. AWS Account ID: 590183820535
5. Region: us-east-1

## Deployment Steps

### Quick Deploy (Recommended)
```powershell
cd "d:\sicgrad\dbt and ml\ml"
.\deploy.ps1
```

The script will:
1. Stop local server
2. Build Docker image
3. Test locally on port 8001
4. Push to ECR
5. Stop old ECS service
6. Deploy new version
7. Wait for stabilization

### Manual Deployment

#### 1. Build and Test Locally
```powershell
docker build -t flight-pricing-api:latest .
docker run -d -p 8000:8000 flight-pricing-api:latest
```

Test: http://localhost:8000

#### 2. Push to ECR
```powershell
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 590183820535.dkr.ecr.us-east-1.amazonaws.com
docker tag flight-pricing-api:latest 590183820535.dkr.ecr.us-east-1.amazonaws.com/flight-pricing-api:latest
docker push 590183820535.dkr.ecr.us-east-1.amazonaws.com/flight-pricing-api:latest
```

#### 3. Update ECS Service
```powershell
aws ecs update-service --cluster flight-pricing-cluster --service flight-pricing-service --force-new-deployment --region us-east-1
```

## Rollback
If something goes wrong:
```powershell
.\rollback.ps1
```

## API Endpoints

### Health Check
```
GET /health
```

### Prediction
```
POST /predict
Content-Type: application/json

{
  "searchDate": "2025-11-24",
  "flightDate": "2025-12-08",
  "startingAirport": "JFK",
  "destinationAirport": "LAX",
  "seatsRemaining": 25,
  "totalTravelDistance": 2475.0,
  "durationMinutes": 360,
  "numSegments": 1,
  "carrier": "AA",
  "departureHour": 10,
  "isBasicEconomy": false,
  "isNonStop": true,
  "isRefundable": false,
  "isHoliday": false,
  "fareBasisCode": "Y14",
  "competitor_prices": [450, 500, 550],
  "total_seats": 180
}
```

## Model Features (23 total)
1. DAYS_UNTIL_FLIGHT
2. SEATS_REMAINING
3. TOTAL_TRAVEL_DISTANCE
4. TRAVEL_DURATION_MINUTES
5. NUM_SEGMENTS
6. AIRLINE_CODE (encoded)
7. ORIGIN_CITY (encoded)
8. DEST_CITY (encoded)
9. FLIGHT_YEAR
10. FLIGHT_MONTH
11. FLIGHT_DAY_OF_WEEK
12. IS_WEEKEND
13. IS_HOLIDAY
14. IS_BASIC_ECONOMY
15. IS_NON_STOP
16. IS_REFUNDABLE
17. cabin_category (from fare code)
18. fare_rule_number (from fare code)
19. passenger_type (from fare code)
20. seasonality_proxy (from fare code)
21. has_numeric_rule (from fare code)
22. is_night_fare_proxy (from fare code)
23. is_weekend_fare_proxy (from fare code)

## Dynamic Pricing Factors
- **Demand** (30% weight): Weekend, peak season, booking window
- **Competition** (25% weight): Competitor price comparison
- **Inventory** (20% weight): Fill rate percentage
- **Time** (15% weight): Last-minute surge pricing
- **Seasonality** (10% weight): Peak/off-peak periods

## Monitoring

### View Logs
```powershell
aws logs tail /ecs/flight-pricing --follow --region us-east-1
```

### Check Service Status
```powershell
aws ecs describe-services --cluster flight-pricing-cluster --services flight-pricing-service --region us-east-1
```

### Get Load Balancer URL
```powershell
aws elbv2 describe-load-balancers --region us-east-1 --query 'LoadBalancers[0].DNSName' --output text
```

## Troubleshooting

### Container won't start
Check logs:
```powershell
aws logs tail /ecs/flight-pricing --region us-east-1
```

### Model encoding errors
Verify airport codes exist in label_encoders.pkl. Currently known issue with JFK/ATL - use airport codes from training data.

### API returns 500 errors
Check feature vector in logs - ensure all 23 features have valid values.

## Notes
- Container uses 512 CPU units, 1GB memory
- Healthcheck runs every 30 seconds
- Load balancer endpoint serves HTTPS traffic
- Debug logging enabled for ML feature inspection
