"""
Demo Data Generator
===================
Generates realistic flight ticket data for the full pipeline demo.
This mimics the structure of the original Kaggle flight data.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid

from config import (
    AIRPORTS, AIRLINES, ROUTE_DISTANCES, 
    DATA_CONFIG, HOLIDAYS
)


def get_distance(origin: str, dest: str) -> int:
    """Get distance between two airports (miles)"""
    key = (origin, dest)
    reverse_key = (dest, origin)
    
    if key in ROUTE_DISTANCES:
        return ROUTE_DISTANCES[key]
    elif reverse_key in ROUTE_DISTANCES:
        return ROUTE_DISTANCES[reverse_key]
    else:
        return random.randint(400, 2500)


def generate_travel_duration(distance: int, num_segments: int) -> str:
    """Generate ISO 8601 duration string (PT#H#M)"""
    hours = distance / 500
    minutes = (num_segments - 1) * 45 + random.randint(-15, 30)
    total_minutes = int(hours * 60 + minutes)
    
    h = total_minutes // 60
    m = total_minutes % 60
    return f"PT{h}H{m}M"


def generate_base_fare(distance: int, days_until: int, seats: int, is_holiday: bool) -> float:
    """Generate realistic base fare based on multiple factors"""
    # Base price per mile 
    if distance < 500:
        price_per_mile = random.uniform(0.25, 0.40)
    elif distance < 1500:
        price_per_mile = random.uniform(0.15, 0.25)
    else:
        price_per_mile = random.uniform(0.10, 0.18)
    
    base = distance * price_per_mile + random.uniform(50, 100)
    
    # Days until flight pricing 
    if days_until <= 3:
        base *= random.uniform(1.8, 2.5)
    elif days_until <= 7:
        base *= random.uniform(1.4, 1.8)
    elif days_until <= 14:
        base *= random.uniform(1.1, 1.4)
    elif days_until <= 21:
        base *= random.uniform(0.95, 1.1)
    else:
        base *= random.uniform(0.8, 1.0)
    
    # Seat scarcity
    if seats < 10:
        base *= random.uniform(1.3, 1.6)
    elif seats < 30:
        base *= random.uniform(1.1, 1.3)
    
    # Holiday premium
    if is_holiday:
        base *= random.uniform(1.2, 1.5)
    
    # Add some noise
    base *= random.uniform(0.9, 1.1)
    
    return round(base, 2)


def generate_segment_data(origin: str, dest: str, departure_time: datetime, num_segments: int, distance: int):
    """Generate segment-level data for a flight"""
    
    airports = list(AIRPORTS.keys())
    carriers = list(AIRLINES.keys())
    cabin_codes = ['coach', 'business', 'first']
    equipment = ['Boeing 737', 'Airbus A320', 'Boeing 777', 'Airbus A321', 'Embraer E175']
    
    if num_segments == 1:
        # Non-stop flight
        segment_airports_dep = [origin]
        segment_airports_arr = [dest]
        segment_carriers = [random.choice(carriers)]
        segment_distances = [distance]
    else:
        # Connecting flight 
        connection = random.choice([a for a in airports if a not in [origin, dest]])
        
        if num_segments == 2:
            segment_airports_dep = [origin, connection]
            segment_airports_arr = [connection, dest]
            dist1 = get_distance(origin, connection)
            dist2 = get_distance(connection, dest)
            segment_distances = [dist1, dist2]
        else:
            # 3 segments
            conn2 = random.choice([a for a in airports if a not in [origin, dest, connection]])
            segment_airports_dep = [origin, connection, conn2]
            segment_airports_arr = [connection, conn2, dest]
            segment_distances = [
                get_distance(origin, connection),
                get_distance(connection, conn2),
                get_distance(conn2, dest)
            ]
        
        # Use same carrier or code-share
        primary_carrier = random.choice(carriers)
        if random.random() < 0.7:
            segment_carriers = [primary_carrier] * num_segments
        else:
            segment_carriers = [random.choice(carriers) for _ in range(num_segments)]
    
    # Generate timestamps
    segment_dep_times = []
    segment_arr_times = []
    segment_durations = []
    
    current_time = departure_time
    for i, dist in enumerate(segment_distances):
        flight_time = timedelta(hours=dist/500)
        arrival_time = current_time + flight_time
        
        segment_dep_times.append(int(current_time.timestamp()))
        segment_arr_times.append(int(arrival_time.timestamp()))
        segment_durations.append(int(flight_time.total_seconds()))
        
        if i < len(segment_distances) - 1:
            current_time = arrival_time + timedelta(minutes=random.randint(45, 120))
        else:
            current_time = arrival_time
    
    return {
        'segmentsDepartureTimeEpochSeconds': '||'.join(map(str, segment_dep_times)),
        'segmentsDepartureTimeRaw': '||'.join([datetime.fromtimestamp(t).isoformat() for t in segment_dep_times]),
        'segmentsArrivalTimeEpochSeconds': '||'.join(map(str, segment_arr_times)),
        'segmentsArrivalTimeRaw': '||'.join([datetime.fromtimestamp(t).isoformat() for t in segment_arr_times]),
        'segmentsArrivalAirportCode': '||'.join(segment_airports_arr),
        'segmentsDepartureAirportCode': '||'.join(segment_airports_dep),
        'segmentsAirlineName': '||'.join([AIRLINES.get(c, c) for c in segment_carriers]),
        'segmentsAirlineCode': '||'.join(segment_carriers),
        'segmentsEquipmentDescription': '||'.join([random.choice(equipment) for _ in range(num_segments)]),
        'segmentsDurationInSeconds': '||'.join(map(str, segment_durations)),
        'segmentsDistance': '||'.join(map(str, segment_distances)),
        'segmentsCabinCode': '||'.join([random.choice(cabin_codes) for _ in range(num_segments)]),
    }


def generate_flights(num_flights: int = 1000) -> pd.DataFrame:
    """Generate realistic flight ticket data"""
    print(f"Generating {num_flights} flight records...")
    
    flights = []
    airports_list = list(AIRPORTS.keys())
    holiday_dates = set(HOLIDAYS)
    
    for i in range(num_flights):
        # Random origin/destination
        origin, dest = random.sample(airports_list, 2)
        
        # Search date 
        search_date = datetime.now() - timedelta(days=random.randint(0, DATA_CONFIG['search_date_range']))
        
        # Flight date
        days_until = random.choices(
            [1, 2, 3, 7, 14, 21, 30, 45, 60],
            weights=[5, 5, 5, 15, 20, 20, 15, 10, 5]
        )[0]
        days_until += random.randint(-2, 2)
        days_until = max(1, min(days_until, 60))
        
        flight_date = search_date + timedelta(days=days_until)
        
        # Check if holiday
        is_holiday = flight_date.strftime('%Y-%m-%d') in holiday_dates
        
        # Get distance
        distance = get_distance(origin, dest)
        
        # Number of segments
        if distance < 800:
            num_segments = random.choices([1, 2], weights=[85, 15])[0]
        elif distance < 1500:
            num_segments = random.choices([1, 2], weights=[70, 30])[0]
        else:
            num_segments = random.choices([1, 2, 3], weights=[50, 40, 10])[0]
        
        # Seats remaining
        seats = random.choices(
            [random.randint(1, 9), random.randint(10, 50), random.randint(51, 150)],
            weights=[20, 50, 30]
        )[0]
        
        # Generate fares
        base_fare = generate_base_fare(distance, days_until, seats, is_holiday)
        taxes = round(base_fare * random.uniform(0.10, 0.20), 2)
        total_fare = round(base_fare + taxes, 2)
        
        # Generate departure time
        hour = random.choices(
            [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21],
            weights=[5, 10, 15, 10, 8, 8, 8, 8, 8, 10, 12, 15, 10, 8, 5, 3]
        )[0]
        departure_time = flight_date.replace(hour=hour, minute=random.randint(0, 59))
        
        # Generate segment data
        segment_data = generate_segment_data(origin, dest, departure_time, num_segments, distance)
        
        # Create flight record
        flight = {
            'legId': str(uuid.uuid4()),
            'searchDate': search_date.strftime('%Y-%m-%d'),
            'flightDate': flight_date.strftime('%Y-%m-%d'),
            'startingAirport': origin,
            'destinationAirport': dest,
            'isBasicEconomy': random.random() < 0.15,
            'isRefundable': random.random() < 0.20,
            'isNonStop': num_segments == 1,
            'baseFare': base_fare,
            'totalFare': total_fare,
            'seatsRemaining': seats,
            'totalTravelDistance': distance,
            'elapsedDays': days_until,
            'travelDuration': generate_travel_duration(distance, num_segments),
            'fareBasisCode': f"{'Y' if random.random() < 0.7 else 'B'}{random.randint(1,9)}{random.choice(['N', 'R', 'K'])}",
            **segment_data
        }
        
        flights.append(flight)
    
    df = pd.DataFrame(flights)
    print(f"Generated {len(df)} flights (${df['totalFare'].min():.0f}-${df['totalFare'].max():.0f}, avg ${df['totalFare'].mean():.0f})")
    
    return df


def save_to_csv(df: pd.DataFrame, filename: str = 'demo_flights.csv') -> str:
    """Save generated data to CSV"""
    from config import BASE_DIR
    
    output_path = BASE_DIR / 'data' / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False)
    print(f"Saved to {output_path}")
    
    return str(output_path)


if __name__ == '__main__':
    df = generate_flights(num_flights=DATA_CONFIG['num_flights'])
    
    csv_path = save_to_csv(df)
    
    print(f"\nData generation complete!")
    print(f"   Run 'python demo_ingest.py' to upload to Snowflake")
