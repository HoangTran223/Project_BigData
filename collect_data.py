"""
Script to collect air quality data from OpenAQ API v3
for alive monitoring stations in Southeast Asia and send to Kafka.

Usage:
    python collect_data.py --days 365 --mode realtime
    python collect_data.py --days 365 --mode historical
"""

import requests
import argparse
from datetime import datetime, timedelta, timezone
from tqdm import tqdm
import time
import sys
import json
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError
from aqi_calculator import calculate_aqi_for_pollutant, convert_unit_to_standard, get_aqi_category
import threading

# --- Constants ---
API_KEYS = [
    "9a88aa20c56e584dcb6d0dd9e3af602113d4daa95f38f787d5efe4466c07ee9b",
    "f9b624320b801f9a419ba600ddf0fa315f71ec88a2be36e372e0ce6553df8c75",
    "8fd7e4792332055fcdebf76f022b408f67a08afee31beae718df358628f20869"
]
BASE_URL = "https://api.openaq.org/v3"
ALIVE_THRESHOLD_DAYS = 7
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW = "openaq-raw-measurements"

# Southeast Asia countries
SOUTHEAST_ASIA_COUNTRIES = ["BN", "KH", "ID", "LA", "MY", "MM", "PH", "SG", "TH", "TL", "VN"]

# Parameters we're interested in
PARAMETERS = ["pm25", "pm10", "o3", "co", "so2", "no2"]

# API key rotation for load balancing
api_key_index = 0
api_key_lock = threading.Lock()

def get_next_api_key():
    """Get next API key in rotation."""
    global api_key_index
    with api_key_lock:
        key = API_KEYS[api_key_index]
        api_key_index = (api_key_index + 1) % len(API_KEYS)
        return key


def get_alive_stations():
    """Get list of alive monitoring stations in Southeast Asia."""
    print("Fetching alive stations in Southeast Asia...")
    
    alive_threshold = datetime.now(timezone.utc) - timedelta(days=ALIVE_THRESHOLD_DAYS)
    alive_stations = []
    
    for country_code in tqdm(SOUTHEAST_ASIA_COUNTRIES, desc="Scanning countries"):
        page = 1
        session = requests.Session()
        session.headers.update({"X-API-Key": get_next_api_key()})
        
        while True:
            try:
                response = session.get(
                    f"{BASE_URL}/locations",
                    params={"iso": country_code, "limit": 1000, "page": page}
                )
                
                if response.status_code == 429:
                    time.sleep(2)
                    # Rotate API key on rate limit
                    session.headers.update({"X-API-Key": get_next_api_key()})
                    continue
                    
                response.raise_for_status()
                data = response.json()
                results = data.get("results", [])
                
                if not results:
                    break
                
                for station in results:
                    datetime_last = station.get("datetimeLast")
                    if datetime_last and datetime_last.get("utc"):
                        last_update = datetime.fromisoformat(datetime_last["utc"])
                        if last_update > alive_threshold:
                            # Get sensors with our parameters of interest
                            sensors = station.get("sensors", [])
                            station_params = [s["parameter"]["name"] for s in sensors]
                            
                            # Only include if has at least PM2.5 or PM10
                            if any(p in station_params for p in ["pm25", "pm10"]):
                                alive_stations.append({
                                    "location_id": station["id"],
                                    "name": station.get("name", "Unknown"),
                                    "country": station["country"]["code"],
                                    "latitude": station["coordinates"]["latitude"],
                                    "longitude": station["coordinates"]["longitude"],
                                    "sensors": sensors
                                })
                
                if len(results) < 1000:
                    break
                page += 1
                time.sleep(0.1)
                
            except Exception as e:
                print(f"\nError fetching {country_code}: {e}")
                break
    
    print(f"\nFound {len(alive_stations)} alive stations with PM data")
    return alive_stations


def fetch_measurements_for_sensor(sensor_id, days_back=None, date_from=None, date_to=None):
    """Fetch hourly measurements for a specific sensor."""
    session = requests.Session()
    session.headers.update({"X-API-Key": get_next_api_key()})
    
    if date_to is None:
        date_to = datetime.now(timezone.utc)
    if date_from is None:
        date_from = date_to - timedelta(days=days_back) if days_back else date_to - timedelta(days=1)
    
    all_measurements = []
    page = 1
    max_pages = 100  # Increased limit for historical data
    
    while page <= max_pages:
        try:
            response = session.get(
                f"{BASE_URL}/sensors/{sensor_id}/measurements/hourly",
                params={
                    "datetime_from": date_from.isoformat(),
                    "datetime_to": date_to.isoformat(),
                    "limit": 1000,
                    "page": page
                }
            )
            
            if response.status_code == 429:
                time.sleep(2)
                # Rotate API key on rate limit
                session.headers.update({"X-API-Key": get_next_api_key()})
                continue
            
            if response.status_code != 200:
                break
                
            data = response.json()
            results = data.get("results", [])
            
            if not results:
                break
            
            for measurement in results:
                period = measurement.get("period", {})
                datetime_from = period.get("datetimeFrom", {})
                if datetime_from and datetime_from.get("utc"):
                    all_measurements.append({
                        "datetime": datetime_from["utc"],
                        "value": measurement.get("value")
                    })
            
            if len(results) < 1000:
                break
            page += 1
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Error fetching sensor {sensor_id}: {e}")
            break
    
    return all_measurements


def send_to_kafka(producer, data):
    """Send data to Kafka topic."""
    try:
        message = json.dumps(data).encode('utf-8')
        producer.send(KAFKA_TOPIC_RAW, value=message)
    except KafkaError as e:
        print(f"Error sending to Kafka: {e}")


def collect_historical_data(stations, days_back, producer):
    """Collect historical data for all stations and send to Kafka."""
    print(f"\nCollecting {days_back} days of historical data for {len(stations)} stations...")
    
    total_records = 0
    
    for station in tqdm(stations, desc="Collecting data"):
        location_id = station["location_id"]
        
        # Process each sensor
        for sensor in station["sensors"]:
            param_name = sensor["parameter"]["name"]
            
            # Only collect data for parameters we're interested in
            if param_name not in PARAMETERS:
                continue
            
            sensor_id = sensor["id"]
            measurements = fetch_measurements_for_sensor(sensor_id, days_back=days_back)
            
            for m in measurements:
                # Convert unit if needed
                value = m["value"]
                unit = sensor["parameter"]["units"]
                value_standard = convert_unit_to_standard(value, param_name, unit)
                
                # Calculate AQI
                aqi = calculate_aqi_for_pollutant(value_standard, param_name)
                aqi_category = get_aqi_category(aqi) if aqi else None
                
                # Prepare message
                message = {
                    "datetime": m["datetime"],
                    "location_id": location_id,
                    "location_name": station["name"],
                    "country": station["country"],
                    "latitude": station["latitude"],
                    "longitude": station["longitude"],
                    "parameter": param_name,
                    "value": value,
                    "value_standard": value_standard,
                    "unit": unit,
                    "aqi": aqi,
                    "aqi_category": aqi_category,
                    "ingestion_timestamp": datetime.now(timezone.utc).isoformat()
                }
                
                send_to_kafka(producer, message)
                total_records += 1
    
    producer.flush()
    print(f"\nTotal records sent to Kafka: {total_records}")


def collect_realtime_data(stations, producer, interval_seconds=300):
    """Collect real-time data continuously."""
    print(f"\nStarting real-time data collection (interval: {interval_seconds}s)...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            date_to = datetime.now(timezone.utc)
            date_from = date_to - timedelta(hours=1)  # Last hour
            
            for station in stations:
                location_id = station["location_id"]
                
                for sensor in station["sensors"]:
                    param_name = sensor["parameter"]["name"]
                    
                    if param_name not in PARAMETERS:
                        continue
                    
                    sensor_id = sensor["id"]
                    measurements = fetch_measurements_for_sensor(
                        sensor_id, 
                        date_from=date_from, 
                        date_to=date_to
                    )
                    
                    for m in measurements:
                        value = m["value"]
                        unit = sensor["parameter"]["units"]
                        value_standard = convert_unit_to_standard(value, param_name, unit)
                        aqi = calculate_aqi_for_pollutant(value_standard, param_name)
                        aqi_category = get_aqi_category(aqi) if aqi else None
                        
                        message = {
                            "datetime": m["datetime"],
                            "location_id": location_id,
                            "location_name": station["name"],
                            "country": station["country"],
                            "latitude": station["latitude"],
                            "longitude": station["longitude"],
                            "parameter": param_name,
                            "value": value,
                            "value_standard": value_standard,
                            "unit": unit,
                            "aqi": aqi,
                            "aqi_category": aqi_category,
                            "ingestion_timestamp": datetime.now(timezone.utc).isoformat()
                        }
                        
                        send_to_kafka(producer, message)
            
            producer.flush()
            print(f"[{datetime.now()}] Collected and sent real-time data. Waiting {interval_seconds}s...")
            time.sleep(interval_seconds)
            
    except KeyboardInterrupt:
        print("\nStopping real-time collection...")
        producer.flush()
        producer.close()


def main():
    parser = argparse.ArgumentParser(description='Collect air quality data from OpenAQ and send to Kafka')
    parser.add_argument('--days', type=int, default=30, 
                        help='Number of days to collect for historical mode (default: 30)')
    parser.add_argument('--mode', type=str, choices=['historical', 'realtime'], default='historical',
                        help='Collection mode: historical or realtime (default: historical)')
    parser.add_argument('--interval', type=int, default=300,
                        help='Interval in seconds for real-time mode (default: 300)')
    parser.add_argument('--sample', type=int, default=None,
                        help='Sample only N stations for testing (optional)')
    parser.add_argument('--kafka-bootstrap', type=str, default=KAFKA_BOOTSTRAP_SERVERS,
                        help=f'Kafka bootstrap servers (default: {KAFKA_BOOTSTRAP_SERVERS})')
    
    args = parser.parse_args()
    
    print("="*80)
    print("OpenAQ Data Collection Script (Kafka Producer)")
    print("="*80)
    print(f"Target region: Southeast Asia")
    print(f"Mode: {args.mode}")
    if args.mode == 'historical':
        print(f"Days to collect: {args.days}")
    else:
        print(f"Collection interval: {args.interval}s")
    print(f"Kafka broker: {args.kafka_bootstrap}")
    print(f"Topic: {KAFKA_TOPIC_RAW}")
    print("="*80)
    
    # Initialize Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.kafka_bootstrap,
            value_serializer=lambda v: v,
            acks='all',
            retries=3
        )
        print("Connected to Kafka successfully")
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        print("Make sure Kafka is running: docker-compose up -d kafka")
        sys.exit(1)
    
    # Step 1: Get alive stations
    stations = get_alive_stations()
    
    if not stations:
        print("No alive stations found!")
        sys.exit(1)
    
    # Sample if requested
    if args.sample:
        stations = stations[:args.sample]
        print(f"\nSampling {args.sample} stations for testing")
    
    # Step 2: Collect data based on mode
    if args.mode == 'historical':
        collect_historical_data(stations, args.days, producer)
    else:
        collect_realtime_data(stations, producer, args.interval)
    
    producer.close()
    print("\nCollection complete!")


if __name__ == "__main__":
    main()
