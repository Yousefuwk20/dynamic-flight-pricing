"""
Flight Data Ingestion - Generates random data and loads to Snowflake RAW
Usage: python ingest.py --records 500
"""

import os
import sys
import argparse
import random
import uuid
from datetime import datetime, timedelta

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from snowflake_connector import get_connection


AIRPORTS = ["JFK", "LAX", "ORD", "DFW", "DEN", "SFO", "SEA", "ATL", "MIA", "BOS", "PHX", "IAH", "LAS", "MCO", "EWR", "MSP"]
AIRLINES = [("AA", "American"), ("DL", "Delta"), ("UA", "United"), ("WN", "Southwest"), ("B6", "JetBlue"), ("AS", "Alaska"), ("NK", "Spirit"), ("F9", "Frontier")]
CABINS = ["coach", "premium coach", "business", "first"]
EQUIPMENT = ["Boeing 737-800", "Airbus A320", "Boeing 777-200", "Airbus A321", "Boeing 787-9", "Embraer E175"]
FARE_BASIS = ["YAA7NR", "KAA7NR", "LAA7NR", "VAA7NR", "HAA7NR", "BAA7NR", "MAA7NR", "QAA7NR"]


def generate_record(search_date: datetime) -> dict:
    """Generate one random flight record with all columns"""
    origin = random.choice(AIRPORTS)
    dest = random.choice([a for a in AIRPORTS if a != origin])
    airline_code, airline_name = random.choice(AIRLINES)
    
    flight_date = search_date + timedelta(days=random.randint(1, 90))
    is_nonstop = random.random() < 0.6
    
    total_distance = random.randint(300, 3000)
    duration_seconds = random.randint(3600, 24000)
    duration_minutes = duration_seconds // 60
    seats = random.randint(1, 150)
    base_fare = round(random.uniform(50, 800), 2)
    total_fare = round(base_fare * random.uniform(1.1, 1.2), 2)
    cabin = random.choice(CABINS)
    equipment = random.choice(EQUIPMENT)
    fare_basis = random.choice(FARE_BASIS)
    
    dep_time = flight_date.replace(hour=random.randint(6, 22), minute=random.choice([0, 15, 30, 45]))
    arr_time = dep_time + timedelta(seconds=duration_seconds)
    
    if is_nonstop:
        # Single segment
        dep_airports = origin
        arr_airports = dest
        dep_epoch = str(int(dep_time.timestamp()))
        arr_epoch = str(int(arr_time.timestamp()))
        dep_raw = dep_time.strftime('%Y-%m-%dT%H:%M:%S.000-05:00')
        arr_raw = arr_time.strftime('%Y-%m-%dT%H:%M:%S.000-05:00')
        codes = airline_code
        names = airline_name
        equip = equipment
        durations = str(duration_seconds)
        distances = str(total_distance)
        cabins = cabin
    else:
        # Two segments with connection
        conn_apt = random.choice([a for a in AIRPORTS if a not in [origin, dest]])
        seg1_duration = duration_seconds // 2
        seg2_duration = duration_seconds - seg1_duration
        seg1_distance = total_distance // 2
        seg2_distance = total_distance - seg1_distance
        
        mid_arr_time = dep_time + timedelta(seconds=seg1_duration)
        layover = random.randint(2700, 7200)  # 45 min to 2 hours
        mid_dep_time = mid_arr_time + timedelta(seconds=layover)
        arr_time = mid_dep_time + timedelta(seconds=seg2_duration)
        
        dep_airports = f"{origin}||{conn_apt}"
        arr_airports = f"{conn_apt}||{dest}"
        dep_epoch = f"{int(dep_time.timestamp())}||{int(mid_dep_time.timestamp())}"
        arr_epoch = f"{int(mid_arr_time.timestamp())}||{int(arr_time.timestamp())}"
        dep_raw = f"{dep_time.strftime('%Y-%m-%dT%H:%M:%S.000-05:00')}||{mid_dep_time.strftime('%Y-%m-%dT%H:%M:%S.000-05:00')}"
        arr_raw = f"{mid_arr_time.strftime('%Y-%m-%dT%H:%M:%S.000-05:00')}||{arr_time.strftime('%Y-%m-%dT%H:%M:%S.000-05:00')}"
        codes = f"{airline_code}||{airline_code}"
        names = f"{airline_name}||{airline_name}"
        equip = f"{equipment}||{random.choice(EQUIPMENT)}"
        durations = f"{seg1_duration}||{seg2_duration}"
        distances = f"{seg1_distance}||{seg2_distance}"
        cabins = f"{cabin}||{cabin}"
    
    return {
        'legId': uuid.uuid4().hex[:16].upper(),
        'searchDate': search_date.strftime('%Y-%m-%d'),
        'flightDate': flight_date.strftime('%Y-%m-%d'),
        'startingAirport': origin,
        'destinationAirport': dest,
        'fareBasisCode': fare_basis,
        'travelDuration': f"PT{duration_minutes // 60}H{duration_minutes % 60}M",
        'elapsedDays': 0,
        'isBasicEconomy': random.random() < 0.25,
        'isRefundable': random.random() < 0.15,
        'isNonStop': is_nonstop,
        'baseFare': base_fare,
        'totalFare': total_fare,
        'seatsRemaining': seats,
        'totalTravelDistance': total_distance,
        'segmentsDepartureTimeEpochSeconds': dep_epoch,
        'segmentsDepartureTimeRaw': dep_raw,
        'segmentsArrivalTimeEpochSeconds': arr_epoch,
        'segmentsArrivalTimeRaw': arr_raw,
        'segmentsArrivalAirportCode': arr_airports,
        'segmentsDepartureAirportCode': dep_airports,
        'segmentsAirlineName': names,
        'segmentsAirlineCode': codes,
        'segmentsEquipmentDescription': equip,
        'segmentsDurationInSeconds': durations,
        'segmentsDistance': distances,
        'segmentsCabinCode': cabins,
    }


def generate_batch(num_records: int) -> pd.DataFrame:
    """Generate batch of random flight records"""
    search_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return pd.DataFrame([generate_record(search_date) for _ in range(num_records)])


def load_to_snowflake(df: pd.DataFrame) -> int:
    """Load DataFrame to Snowflake RAW.FLIGHTS"""
    if df.empty:
        return 0
    
    # Convert column names to uppercase for Snowflake compatibility
    df.columns = [col.upper() for col in df.columns]
    
    with get_connection('RAW') as conn:
        conn.cursor().execute("""
            CREATE TABLE IF NOT EXISTS RAW.FLIGHTS (
                LEGID VARCHAR(50) PRIMARY KEY,
                SEARCHDATE DATE,
                FLIGHTDATE DATE,
                STARTINGAIRPORT VARCHAR(10),
                DESTINATIONAIRPORT VARCHAR(10),
                FAREBASISCODE VARCHAR(20),
                TRAVELDURATION VARCHAR(20),
                ELAPSEDDAYS INTEGER,
                ISBASICECONOMY BOOLEAN,
                ISREFUNDABLE BOOLEAN,
                ISNONSTOP BOOLEAN,
                BASEFARE DECIMAL(10,2),
                TOTALFARE DECIMAL(10,2),
                SEATSREMAINING INTEGER,
                TOTALTRAVELDISTANCE DECIMAL(10,2),
                SEGMENTSDEPARTURETIMEEPOCHSECONDS VARCHAR(200),
                SEGMENTSDEPARTURETIMERAW VARCHAR(200),
                SEGMENTSARRIVALTIMEEPOCHSECONDS VARCHAR(200),
                SEGMENTSARRIVALTIMERAW VARCHAR(200),
                SEGMENTSARRIVALAIRPORTCODE VARCHAR(100),
                SEGMENTSDEPARTUREAIRPORTCODE VARCHAR(100),
                SEGMENTSAIRLINENAME VARCHAR(200),
                SEGMENTSAIRLINECODE VARCHAR(50),
                SEGMENTSEQUIPMENTDESCRIPTION VARCHAR(200),
                SEGMENTSDURATIONINSECONDS VARCHAR(100),
                SEGMENTSDISTANCE VARCHAR(100),
                SEGMENTSCABINCODE VARCHAR(100),
                _LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        from snowflake.connector.pandas_tools import write_pandas
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name='FLIGHTS',
            schema='RAW',
            quote_identifiers=False
        )
        return nrows


def main():
    parser = argparse.ArgumentParser(description='Ingest random flight data to Snowflake')
    parser.add_argument('--records', type=int, default=1000, help='Number of records')
    args = parser.parse_args()
    
    df = generate_batch(args.records)
    
    rows = load_to_snowflake(df)
    
    print(f"Loaded {rows} records to RAW.FLIGHTS")


if __name__ == '__main__':
    main()
