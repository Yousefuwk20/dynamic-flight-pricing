import numpy as np
import pickle
import xgboost as xgb
from datetime import datetime
from typing import Dict

PRICING_CONFIG = {
    'demand_weight': 0.30,
    'competition_weight': 0.25,
    'inventory_weight': 0.20,
    'time_weight': 0.15,
    'seasonality_weight': 0.10,
}

FEATURE_ORDER = [
    'DAYS_UNTIL_FLIGHT', 'SEATS_REMAINING', 'TOTAL_TRAVEL_DISTANCE',
    'TRAVEL_DURATION_MINUTES', 'NUM_SEGMENTS', 'AIRLINE_CODE',
    'ORIGIN_CITY', 'DEST_CITY', 'FLIGHT_YEAR', 'FLIGHT_MONTH',
    'FLIGHT_DAY_OF_WEEK', 'IS_WEEKEND', 'IS_HOLIDAY',
    'IS_BASIC_ECONOMY', 'IS_NON_STOP', 'IS_REFUNDABLE',
    'cabin_category', 'fare_rule_number', 'passenger_type',
    'seasonality_proxy', 'has_numeric_rule', 'is_night_fare_proxy',
    'is_weekend_fare_proxy'
]


# LOAD MODEL & ENCODERS

def load_model_and_encoders(model_path='flight_pricing_model.json', 
                            encoders_path='label_encoders.pkl'):
    """Load XGBoost model and label encoders"""
    model = xgb.Booster()
    model.load_model(model_path)
    
    # Load encoders
    with open(encoders_path, 'rb') as f:
        encoders = pickle.load(f)
    
    return model, encoders


# ML PREDICTION

def parse_fare_code(fare_code: str, flight_data: dict = None) -> dict:
    """Parse fare basis code to extract features for 23-feature model"""
    if flight_data is None:
        flight_data = {}
    
    if not fare_code:
        return {
            'cabin_category': 1,
            'fare_rule_number': 0,
            'passenger_type': 0,
            'seasonality_proxy': 1,
            'has_numeric_rule': 0,
            'is_night_fare_proxy': 0,
            'is_weekend_fare_proxy': 0
        }
    
    import re
    fc = fare_code.upper()
    
    # Cabin mapping (ordinal)
    cabin_map = {
        'F': 5, 'A': 5, 'P': 5,  # First
        'J': 4, 'C': 4, 'D': 4, 'I': 4, 'Z': 4,  # Business
        'W': 3,  # Premium Economy
        'Y': 2, 'B': 2, 'M': 2, 'H': 2  # Full Economy
    }
    cabin_category = cabin_map.get(fc[0], 1)
    
    # Numeric rule extraction
    m = re.search(r'(\d+)', fc)
    fare_rule_number = int(m.group(1)) if m else 0
    has_numeric_rule = 1 if m else 0
    
    # Passenger type
    if 'CH' in fc:
        passenger_type = 1
    elif 'IN' in fc or 'INF' in fc:
        passenger_type = 2
    else:
        passenger_type = 0
    
    # Seasonality proxy
    if 'H' in fc and 'LH' not in fc:
        seasonality_proxy = 2
    elif 'L' in fc and 'LH' not in fc:
        seasonality_proxy = 0
    else:
        seasonality_proxy = 1
    
    # Night fare proxy
    dep_hour = flight_data.get('departureHour')
    if dep_hour is not None:
        is_night = 1 if (dep_hour < 6 or dep_hour >= 22) else 0
    else:
        is_night = 1 if 'N' in fc else 0
    
    # Weekend fare proxy
    flight_day = flight_data.get('flightDate')
    if flight_day:
        try:
            dow = datetime.strptime(flight_day, '%Y-%m-%d').weekday()
            is_weekend = 1 if dow >= 5 else 0
        except:
            is_weekend = 1 if ('WE' in fc or 'WK' in fc) else 0
    else:
        is_weekend = 1 if ('WE' in fc or 'WK' in fc) else 0
    
    return {
        'cabin_category': cabin_category,
        'fare_rule_number': fare_rule_number,
        'passenger_type': passenger_type,
        'seasonality_proxy': seasonality_proxy,
        'has_numeric_rule': has_numeric_rule,
        'is_night_fare_proxy': is_night,
        'is_weekend_fare_proxy': is_weekend
    }


def create_ml_features(flight_data: dict) -> dict:
    """Transform user input to 23 ML features"""
    # Parse dates
    search_date = datetime.strptime(flight_data['searchDate'], '%Y-%m-%d')
    flight_date = datetime.strptime(flight_data['flightDate'], '%Y-%m-%d')
    
    days_until_flight = max(0, (flight_date - search_date).days)
    flight_year = flight_date.year
    flight_month = flight_date.month
    flight_weekday = flight_date.weekday()
    is_weekend = 1 if flight_weekday >= 5 else 0
    
    # Parse fare basis code
    fare_info = parse_fare_code(flight_data.get('fareBasisCode', ''), flight_data)
    
    # Build 23-feature dict - categorical fields as strings for encoding
    features = {
        'DAYS_UNTIL_FLIGHT': days_until_flight,
        'SEATS_REMAINING': int(flight_data.get('seatsRemaining', 50)),
        'TOTAL_TRAVEL_DISTANCE': float(flight_data.get('totalTravelDistance', 1000)),
        'TRAVEL_DURATION_MINUTES': float(flight_data.get('durationMinutes', 180)),
        'NUM_SEGMENTS': int(flight_data.get('numSegments', 1)),
        'AIRLINE_CODE': flight_data.get('airlineCode', flight_data.get('carrier', 'DL')),
        'ORIGIN_CITY': flight_data.get('originCity', flight_data.get('startingAirport', 'JFK')),
        'DEST_CITY': flight_data.get('destCity', flight_data.get('destinationAirport', 'LAX')),
        'FLIGHT_YEAR': flight_year,
        'FLIGHT_MONTH': flight_month,
        'FLIGHT_DAY_OF_WEEK': flight_weekday,
        'IS_WEEKEND': is_weekend,
        'IS_HOLIDAY': int(flight_data.get('IS_HOLIDAY', 0)),
        'IS_BASIC_ECONOMY': int(flight_data.get('isBasicEconomy', False)),
        'IS_NON_STOP': int(flight_data.get('isNonStop', True)),
        'IS_REFUNDABLE': int(flight_data.get('isRefundable', False)),
        'cabin_category': fare_info['cabin_category'],
        'fare_rule_number': fare_info['fare_rule_number'],
        'passenger_type': fare_info['passenger_type'],
        'seasonality_proxy': fare_info['seasonality_proxy'],
        'has_numeric_rule': fare_info['has_numeric_rule'],
        'is_night_fare_proxy': fare_info['is_night_fare_proxy'],
        'is_weekend_fare_proxy': fare_info['is_weekend_fare_proxy']
    }
    
    return features


def get_ml_prediction(model, encoders, features: dict) -> float:
    """Get prediction from XGBoost model with proper encoding"""
    encoded_features = features.copy()
    
    string_categorical_cols = ['AIRLINE_CODE', 'ORIGIN_CITY', 'DEST_CITY']
    for col in string_categorical_cols:
        if col in encoders and col in encoded_features:
            try:
                val = str(encoded_features[col])
                encoded_features[col] = int(encoders[col].transform([val])[0])
            except:
                encoded_features[col] = 0
    
    for col in ['cabin_category', 'passenger_type', 'seasonality_proxy']:
        if col in encoded_features:
            try:
                encoded_features[col] = int(encoded_features[col])
            except:
                encoded_features[col] = 0
    
    # Build ordered feature array
    feature_values = []
    for f in FEATURE_ORDER:
        v = encoded_features.get(f, 0)
        if isinstance(v, bool):
            v = int(v)
        try:
            feature_values.append(float(v))
        except:
            feature_values.append(0.0)
    
    dmatrix = xgb.DMatrix([feature_values], feature_names=FEATURE_ORDER)
    
    prediction = model.predict(dmatrix)
    
    return float(prediction[0])


# DYNAMIC PRICING FACTORS

def calculate_demand_factor(context: Dict) -> float:
    """Demand factor: High demand = higher price"""
    demand_score = 0.0
    
    if context.get('is_weekend', 0) == 1:
        demand_score += 0.10
    
    flight_month = context.get('flight_month', 6)
    if flight_month in [6, 7, 8]:  # Summer
        demand_score += 0.15
    elif flight_month in [12, 1]:  # Winter holidays
        demand_score += 0.15
    
    flight_weekday = context.get('flight_weekday', 3)
    if flight_weekday in [5, 6]:
        demand_score += 0.10
    
    days_until = context.get('days_until_flight', 30)
    if 7 <= days_until <= 21:
        demand_score += 0.10
    
    return np.clip(demand_score, -0.20, 0.50)


def calculate_inventory_factor(context: Dict) -> float:
    """
    FIXED: High fill rate (few seats left) = HIGHER price (scarcity)
           Low fill rate (many seats left) = LOWER price (need to fill)
    Uses percentage-based logic for better sensitivity to total_seats changes
    """
    seats_remaining = context.get('seats_remaining', 50)
    total_seats = context.get('total_seats', 180)
    days_until_flight = context.get('days_until_flight', 30)
    
    fill_rate = (total_seats - seats_remaining) / total_seats if total_seats > 0 else 0
    seats_remaining_pct = (seats_remaining / total_seats) * 100 if total_seats > 0 else 50
    
    adjustment = 0.0
    
    # Use percentage-based thresholds for better sensitivity
    if seats_remaining_pct <= 2:  
        adjustment = 0.50  
    elif seats_remaining_pct <= 5:  
        adjustment = 0.40
    elif seats_remaining_pct <= 10:  
        adjustment = 0.25
    elif seats_remaining_pct <= 15:  
        adjustment = 0.15
    elif seats_remaining_pct >= 60:  
        adjustment = -0.25  
    elif seats_remaining_pct >= 50:  
        adjustment = -0.15
    elif seats_remaining_pct >= 40: 
        adjustment = -0.10
    
    return adjustment


def calculate_time_factor(context: Dict) -> float:
    """Time factor: Last minute = surge pricing"""
    days_until_flight = context.get('days_until_flight', 30)
    
    if days_until_flight <= 1:
        return 0.50
    elif days_until_flight <= 3:
        return 0.30
    elif days_until_flight <= 7:
        return 0.20
    elif days_until_flight <= 14:
        return 0.10
    elif days_until_flight >= 60:
        return -0.10 
    else:
        return 0.0


def calculate_competition_factor(context: Dict) -> float:
    """Competition factor: If competitors are cheaper, we LOWER price. If we're cheaper, we RAISE slightly."""
    competitor_prices = context.get('competitor_prices', [])
    
    if not competitor_prices:
        return 0.0
    
    our_price = context.get('current_price', context.get('ml_prediction', 0))
    
    if our_price == 0:
        return 0.0
    
    avg_competitor = np.mean(competitor_prices)
    price_diff_pct = (our_price - avg_competitor) / avg_competitor
    
    adjustment = 0.0
    
    if price_diff_pct > 0.15:
        adjustment = -0.20  
    elif price_diff_pct > 0.10:
        adjustment = -0.15  
    elif price_diff_pct > 0.05:
        adjustment = -0.08  
    elif price_diff_pct < -0.10:
        adjustment = 0.10  
    elif price_diff_pct < -0.05:
        adjustment = 0.05  
    
    return adjustment


def calculate_seasonality_factor(context: Dict) -> float:
    """Seasonality factor"""
    season = context.get('season', 'standard')
    
    season_map = {
        'peak_summer': 0.20,
        'peak_winter': 0.15,
        'shoulder': 0.05,
        'standard': 0.0,
        'off_season': -0.10
    }
    
    return season_map.get(season, 0.0)


def calculate_dynamic_price(ml_prediction: float, context: Dict, config: Dict = None) -> Dict:
    """Calculate final price with dynamic adjustments"""
    if config is None:
        config = PRICING_CONFIG
    
    # Calculate factors
    demand_factor = calculate_demand_factor(context)
    competition_factor = calculate_competition_factor(context)
    inventory_factor = calculate_inventory_factor(context)
    time_factor = calculate_time_factor(context)
    seasonality_factor = calculate_seasonality_factor(context)
    
    # Weighted total adjustment
    weighted_demand = demand_factor * config['demand_weight']
    weighted_competition = competition_factor * config['competition_weight']
    weighted_inventory = inventory_factor * config['inventory_weight']
    weighted_time = time_factor * config['time_weight']
    weighted_seasonality = seasonality_factor * config['seasonality_weight']
    
    total_adjustment = (weighted_demand + weighted_competition + weighted_inventory + 
                       weighted_time + weighted_seasonality)
    
    # Apply adjustment
    adjusted_price = ml_prediction * (1 + total_adjustment)
    
    # Bounds
    min_price = ml_prediction * 0.7  # Max 30% decrease
    max_price = ml_prediction * 1.5  # Max 50% increase
    final_price = np.clip(adjusted_price, min_price, max_price)
    
    final_price = round(final_price)
    
    return {
        'final_price': final_price,
        'ml_prediction': ml_prediction,
        'total_adjustment': (final_price - ml_prediction) / ml_prediction * 100,
        'adjustments': {
            'demand': demand_factor,
            'competition': competition_factor,
            'inventory': inventory_factor,
            'time': time_factor,
            'seasonality': seasonality_factor
        },
        'factors_applied': {
            'demand': f"{demand_factor*100:+.1f}%",
            'competition': f"{competition_factor*100:+.1f}%",
            'inventory': f"{inventory_factor*100:+.1f}%",
            'time': f"{time_factor*100:+.1f}%",
            'seasonality': f"{seasonality_factor*100:+.1f}%",
        }
    }


def create_pricing_context(ml_features: dict, market_data: dict = None) -> dict:
    """Create context for dynamic pricing"""
    context = {
        'seats_remaining': ml_features['SEATS_REMAINING'],
        'total_seats': market_data.get('total_seats', 180) if market_data else 180,
        'days_until_flight': ml_features['DAYS_UNTIL_FLIGHT'],
        'flight_weekday': ml_features['FLIGHT_DAY_OF_WEEK'],
        'flight_month': ml_features['FLIGHT_MONTH'],
        'is_weekend': ml_features['IS_WEEKEND'],
        'seasonality_encoded': ml_features['seasonality_proxy'],
        'competitor_prices': market_data.get('competitor_prices', []) if market_data else [],
        'season': market_data.get('season', 'standard') if market_data else 'standard',
    }
    
    if market_data and 'ml_prediction' in market_data:
        context['ml_prediction'] = market_data['ml_prediction']
    
    return context


def calculate_confidence(features: dict, ml_prediction: float) -> str:
    """Calculate confidence based on model certainty and data quality"""
    score = 0
    
    
    popular_routes = ['JFK-LAX', 'LAX-JFK', 'ORD-LAX', 'JFK-SFO', 'ATL-LAX']
    route = f"{features.get('ORIGIN_CITY', '')}-{features.get('DEST_CITY', '')}"
    if route in popular_routes or route[::-1] in popular_routes:
        score += 2
    
    # Normal booking window
    days = features['DAYS_UNTIL_FLIGHT']
    if 7 <= days <= 60:
        score += 2
    elif 3 <= days <= 90:
        score += 1
    
    # Reasonable inventory
    seats = features['SEATS_REMAINING']
    if 10 <= seats <= 150:
        score += 1
    
    # Price reasonableness check
    if 100 <= ml_prediction <= 1500:
        score += 1
    
    if score >= 4:
        return 'High'
    elif score >= 2:
        return 'Medium'
    else:
        return 'Low'


def predict_price(model, encoders, flight_data: dict, market_data: dict = None) -> dict:
    """Complete pricing pipeline"""
    ml_features = create_ml_features(flight_data)
    ml_prediction = get_ml_prediction(model, encoders, ml_features)
    
    if market_data is None:
        market_data = {}
    market_data['ml_prediction'] = ml_prediction
    
    pricing_context = create_pricing_context(ml_features, market_data)
    dynamic_result = calculate_dynamic_price(ml_prediction, pricing_context)
    
    breakdown = {
        'base': ml_prediction,
        'demand_adj': ml_prediction * dynamic_result['adjustments']['demand'] * PRICING_CONFIG['demand_weight'],
        'competition_adj': ml_prediction * dynamic_result['adjustments']['competition'] * PRICING_CONFIG['competition_weight'],
        'inventory_adj': ml_prediction * dynamic_result['adjustments']['inventory'] * PRICING_CONFIG['inventory_weight'],
        'time_adj': ml_prediction * dynamic_result['adjustments']['time'] * PRICING_CONFIG['time_weight'],
        'seasonality_adj': ml_prediction * dynamic_result['adjustments']['seasonality'] * PRICING_CONFIG['seasonality_weight'],
    }
    
    return {
        'ml_base_price': ml_prediction,
        'dynamic_price': dynamic_result['final_price'],
        'total_adjustment_pct': dynamic_result['total_adjustment'],
        'factors': dynamic_result['factors_applied'],
        'breakdown': breakdown,
        'confidence': calculate_confidence(ml_features, ml_prediction),
        'features_used': ml_features
    }