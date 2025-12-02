"""
FastAPI backend for Flight Pricing System (XGBoost JSON)
Serves predictions via REST API
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import Optional, Dict, List
import uvicorn
from datetime import datetime

from pricing_system import (
    load_model_and_encoders,
    predict_price
)

app = FastAPI(
    title="Flight Pricing API",
    description="AI-Powered Dynamic Flight Price Prediction",
    version="2.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

model = None
encoders = None


class FlightRequest(BaseModel):
    searchDate: str = Field(..., description="Search date in YYYY-MM-DD format")
    flightDate: str = Field(..., description="Flight date in YYYY-MM-DD format")
    
    startingAirport: str = Field(..., description="3-letter origin airport code")
    destinationAirport: str = Field(..., description="3-letter destination airport code")
    
    seatsRemaining: int = Field(50, ge=0, le=500, description="Number of seats remaining")
    totalTravelDistance: float = Field(1000, ge=0, description="Total travel distance in miles")
    durationMinutes: int = Field(180, ge=0, description="Total flight duration in minutes")
    numSegments: int = Field(1, ge=1, le=6, description="Number of flight segments")
    
    carrier: str = Field("DL", description="Airline carrier code")
    
    departureHour: Optional[int] = Field(10, ge=0, le=23, description="Departure hour (0-23)")
    
    isBasicEconomy: bool = Field(False, description="Is basic economy class")
    isNonStop: bool = Field(True, description="Is non-stop flight")
    isRefundable: bool = Field(False, description="Is ticket refundable")
    isHoliday: bool = Field(False, description="Is holiday period")
    
    fareBasisCode: Optional[str] = Field("Y14", description="Fare basis code for parsing fare attributes")
    
    cabinCategory: Optional[int] = Field(None, description="Cabin category (1-5, 5=First)")
    passengerType: Optional[int] = Field(None, description="Passenger type (0=Adult, 1=Child, 2=Infant)")
    
    competitor_prices: Optional[List[float]] = Field(None, description="List of competitor prices")
    total_seats: Optional[int] = Field(180, description="Total seats on aircraft")


class PredictionResponse(BaseModel):
    success: bool
    ml_base_price: float
    dynamic_price: float
    total_adjustment: float
    confidence: str
    factors: Dict[str, str]
    breakdown: Dict[str, float]
    route: str
    flight_date: str
    features_summary: Optional[Dict] = None


@app.on_event("startup")
async def startup_event():
    """Load model on startup"""
    global model, encoders
    
    model, encoders = load_model_and_encoders(
        model_path='flight_pricing_model.json',
        encoders_path='label_encoders.pkl'
    )


@app.get("/")
async def root():
    """Serve the HTML frontend"""
    try:
        return FileResponse('index.html', media_type='text/html')
    except:
        return {
            "message": "Flight Pricing API",
            "version": "2.0.0",
            "status": "running",
            "features": 23,
            "endpoints": {
                "health": "/health",
                "predict": "/predict",
                "docs": "/docs"
            }
        }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "encoders_loaded": encoders is not None,
        "feature_count": 23,
        "timestamp": datetime.now().isoformat()
    }


@app.post("/predict", response_model=PredictionResponse)
async def predict(request: FlightRequest):
    """
    Predict flight price with dynamic adjustments.
    Uses 23 features for ML prediction.
    """
    if model is None or encoders is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        flight_data = {
            'searchDate': request.searchDate,
            'flightDate': request.flightDate,
            
            'seatsRemaining': request.seatsRemaining,
            'totalTravelDistance': request.totalTravelDistance,
            'durationMinutes': request.durationMinutes,
            'numSegments': request.numSegments,
            
            'departureHour': request.departureHour,
            
            'isBasicEconomy': request.isBasicEconomy,
            'isNonStop': request.isNonStop,
            'isRefundable': request.isRefundable,
            'IS_HOLIDAY': 1 if request.isHoliday else 0,
            
            'fareBasisCode': request.fareBasisCode or 'Y14',
            
            'originCity': request.startingAirport,
            'destCity': request.destinationAirport,
            'airlineCode': request.carrier,
        }
        
        if request.cabinCategory is not None:
            flight_data['cabin_category_override'] = request.cabinCategory
        if request.passengerType is not None:
            flight_data['passenger_type_override'] = request.passengerType
        
        market_data = {
            'competitor_prices': request.competitor_prices or [],
            'total_seats': request.total_seats or 180
        }
        
        result = predict_price(model, encoders, flight_data, market_data)
        
        return PredictionResponse(
            success=True,
            ml_base_price=round(result['ml_base_price'], 2),
            dynamic_price=round(result['dynamic_price'], 2),
            total_adjustment=round(result['total_adjustment_pct'], 2),
            confidence=result['confidence'],
            factors=result['factors'],
            breakdown={k: round(v, 2) for k, v in result['breakdown'].items()},
            route=f"{request.startingAirport} → {request.destinationAirport}",
            flight_date=request.flightDate,
            features_summary={
                'days_until_flight': result['features_used'].get('DAYS_UNTIL_FLIGHT'),
                'cabin_category': result['features_used'].get('cabin_category'),
                'seasonality_proxy': result['features_used'].get('seasonality_proxy'),
                'is_weekend': result['features_used'].get('IS_WEEKEND'),
                'is_holiday': result['features_used'].get('IS_HOLIDAY'),
            }
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid input: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")


@app.post("/predict/batch")
async def predict_batch(requests: List[FlightRequest]):
    """
    Predict prices for multiple flights
    """
    if model is None or encoders is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    results = []
    for req in requests:
        try:
            result = await predict(req)
            results.append(result)
        except Exception as e:
            results.append({
                "success": False,
                "error": str(e),
                "route": f"{req.startingAirport} → {req.destinationAirport}"
            })
    
    return {
        "total": len(requests),
        "successful": sum(1 for r in results if r.get("success", False)),
        "results": results
    }


@app.get("/encoders")
async def get_encoders():
    """Return available encoder mappings for frontend dropdowns"""
    if encoders is None:
        return {"encoders": {}}
    
    encoder_info = {}
    for name, enc in encoders.items():
        try:
            if hasattr(enc, 'classes_'):
                encoder_info[name] = list(enc.classes_)
            else:
                encoder_info[name] = "encoder loaded"
        except:
            encoder_info[name] = "encoder loaded"
    
    return {"encoders": encoder_info}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
