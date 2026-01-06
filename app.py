"""
Flask web application for Air Quality Monitoring Dashboard.
Provides API endpoints and serves the frontend.
"""

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import requests
from datetime import datetime, timedelta, timezone
from aqi_calculator import calculate_aqi_for_pollutant, convert_unit_to_standard, get_aqi_category
import os
import time
import sys
import pickle
import pandas as pd
import numpy as np
from pathlib import Path

# Add ml_training to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'ml_training'))
try:
    from data_loader import AirQualityDataLoader
except ImportError:
    AirQualityDataLoader = None

app = Flask(__name__)
CORS(app)

# OpenAQ API configuration
API_KEYS = [
    "9a88aa20c56e584dcb6d0dd9e3af602113d4daa95f38f787d5efe4466c07ee9b",
    "f9b624320b801f9a419ba600ddf0fa315f71ec88a2be36e372e0ce6553df8c75",
    "8fd7e4792332055fcdebf76f022b408f67a08afee31beae718df358628f20869"
]
BASE_URL = "https://api.openaq.org/v3"

# Southeast Asia countries with full names
COUNTRIES = {
    "BN": "Brunei",
    "KH": "Cambodia",
    "ID": "Indonesia",
    "LA": "Laos",
    "MY": "Malaysia",
    "MM": "Myanmar",
    "PH": "Philippines",
    "SG": "Singapore",
    "TH": "Thailand",
    "TL": "Timor-Leste",
    "VN": "Vietnam"
}

# Parameters we're interested in
PARAMETERS = ["pm25", "pm10", "o3", "co", "so2", "no2"]

api_key_index = 0

def get_next_api_key():
    """Get next API key in rotation."""
    global api_key_index
    key = API_KEYS[api_key_index]
    api_key_index = (api_key_index + 1) % len(API_KEYS)
    return key


def get_recommendation(aqi, aqi_category):
    """
    Get recommendation message based on AQI value and category.
    """
    if aqi <= 50:
        return {
            "message": "Thời tiết tốt, phù hợp để ra ngoài chơi và tập thể dục!",
            "icon": "😊",
            "color": "good"
        }
    elif aqi <= 100:
        return {
            "message": "Chất lượng không khí ở mức chấp nhận được. Có thể ra ngoài nhưng nên hạn chế hoạt động ngoài trời kéo dài.",
            "icon": "😐",
            "color": "moderate"
        }
    elif aqi <= 150:
        return {
            "message": "Không khí không tốt cho người nhạy cảm. Người già, trẻ em và người có vấn đề về hô hấp nên hạn chế ra ngoài.",
            "icon": "😷",
            "color": "unhealthy-sensitive"
        }
    elif aqi <= 200:
        return {
            "message": "Chất lượng không khí xấu. Không nên ra ngoài, đặc biệt là tập thể dục ngoài trời. Nên đóng cửa sổ và sử dụng máy lọc không khí.",
            "icon": "⚠️",
            "color": "unhealthy"
        }
    elif aqi <= 300:
        return {
            "message": "Chất lượng không khí rất xấu! Tuyệt đối không nên ra ngoài. Nên ở trong nhà và sử dụng máy lọc không khí.",
            "icon": "🚨",
            "color": "very-unhealthy"
        }
    else:
        return {
            "message": "CẢNH BÁO: Chất lượng không khí cực kỳ nguy hiểm! Ở trong nhà, đóng tất cả cửa sổ và sử dụng máy lọc không khí.",
            "icon": "☠️",
            "color": "hazardous"
        }


# ML Prediction functions
_model_cache = {}
_data_loader = None

def get_model(model_name='lightgbm'):
    """Load and cache ML model."""
    global _model_cache
    
    if model_name in _model_cache:
        return _model_cache[model_name]
    
    model_path = Path(__file__).parent / 'ml_training' / 'models' / f'{model_name}_global.pkl'
    
    if not model_path.exists():
        return None
    
    try:
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
        _model_cache[model_name] = model_data
        return model_data
    except Exception as e:
        print(f"Error loading model: {e}")
        return None


def get_data_loader():
    """Get or create data loader instance."""
    global _data_loader
    
    if _data_loader is None and AirQualityDataLoader:
        try:
            _data_loader = AirQualityDataLoader()
        except Exception as e:
            # Silently fail - historical data is optional
            # Prediction will work with current data only
            return None
    
    return _data_loader


def create_features_for_prediction(current_data, historical_data=None):
    """
    Create features for prediction from current station data.
    
    Args:
        current_data: Dict with current station data (aqi, parameters, etc.)
        historical_data: Optional dict with lag features {'aqi_lag_1h', 'aqi_lag_24h', 'aqi_lag_168h'}
                        or None to use current AQI
    
    Returns:
        DataFrame with features ready for prediction
    """
    # Create base DataFrame
    now = datetime.now(timezone.utc)
    df = pd.DataFrame([{
        'datetime': now,
        'location_id': current_data.get('location_id'),
        'location_name': current_data.get('location_name', ''),
        'country': current_data.get('country', ''),
        'latitude': current_data.get('latitude', 0),
        'longitude': current_data.get('longitude', 0),
        'aqi': current_data.get('aqi', 0),
        'aqi_category': current_data.get('aqi_category', ''),
        'parameters': current_data.get('parameters', []),
        'values': current_data.get('values', [])
    }])
    
    # Time features
    df['hour'] = df['datetime'].dt.hour
    df['day_of_week'] = df['datetime'].dt.dayofweek
    df['month'] = df['datetime'].dt.month
    df['day_of_year'] = df['datetime'].dt.dayofyear
    df['is_weekend'] = df['datetime'].dt.dayofweek >= 5
    
    # Extract pollutant values
    pollutants = ['pm25', 'pm10', 'o3', 'co', 'so2', 'no2']
    for pollutant in pollutants:
        df[pollutant] = df.apply(
            lambda row: _extract_pollutant_value(row, pollutant),
            axis=1
        )
        df[pollutant] = df[pollutant].fillna(0.0)
    
    # Country encoding (need to use same encoder as training)
    model_data = get_model('lightgbm')
    if model_data and 'country_encoder' in model_data:
        try:
            df['country_encoded'] = model_data['country_encoder'].transform([df['country'].iloc[0]])[0]
        except:
            # If country not in encoder, use 0
            df['country_encoded'] = 0
    else:
        df['country_encoded'] = 0
    
    # Lag features: fetch from API (only 3 specific time points for fast inference)
    # Rolling statistics: use current AQI (not fetched to save API calls)
    if historical_data and isinstance(historical_data, dict):
        # historical_data is now a dict with lag features
        # Use is not None check (not 'or') because 0 is a valid AQI value
        lag_1h = historical_data.get('aqi_lag_1h')
        df['aqi_lag_1h'] = lag_1h if lag_1h is not None else df['aqi'].iloc[0]
        
        lag_24h = historical_data.get('aqi_lag_24h')
        df['aqi_lag_24h'] = lag_24h if lag_24h is not None else df['aqi'].iloc[0]
        
        lag_168h = historical_data.get('aqi_lag_168h')
        df['aqi_lag_168h'] = lag_168h if lag_168h is not None else df['aqi'].iloc[0]
    else:
        # No historical data - use current AQI as default
        df['aqi_lag_1h'] = df['aqi'].iloc[0]
        df['aqi_lag_24h'] = df['aqi'].iloc[0]
        df['aqi_lag_168h'] = df['aqi'].iloc[0]
    
    # Rolling statistics: always use current AQI (not fetched for speed)
    # This matches training behavior when min_periods=1 and only 1 data point available
    df['aqi_mean_7d'] = df['aqi'].iloc[0]
    df['aqi_std_7d'] = 0.0
    df['aqi_max_7d'] = df['aqi'].iloc[0]
    df['aqi_min_7d'] = df['aqi'].iloc[0]
    df['aqi_mean_30d'] = df['aqi'].iloc[0]
    
    return df


def _extract_pollutant_value(row, pollutant):
    """Extract pollutant value from parameters and values arrays."""
    try:
        params = row.get('parameters', [])
        values = row.get('values', [])
        
        if isinstance(params, list) and isinstance(values, list):
            if pollutant in params:
                idx = params.index(pollutant)
                return float(values[idx])
    except (ValueError, IndexError, TypeError):
        pass
    return None


def fetch_lag_features_from_openaq(location_id, sensors):
    """
    Fetch only lag features (1h, 24h, 168h ago) from OpenAQ API for fast inference.
    Only calls API for specific time points needed, not full history.
    
    Args:
        location_id: Location ID
        sensors: List of sensor dicts from location data
    
    Returns:
        Dict with keys: 'aqi_lag_1h', 'aqi_lag_24h', 'aqi_lag_168h' (or None if not available)
    """
    try:
        now = datetime.now(timezone.utc)
        target_times = {
            'aqi_lag_1h': now - timedelta(hours=1),
            'aqi_lag_24h': now - timedelta(hours=24),
            'aqi_lag_168h': now - timedelta(hours=168)
        }
        
        # Round to hour
        for key in target_times:
            target_times[key] = target_times[key].replace(minute=0, second=0, microsecond=0)
        
        lag_aqi = {}
        
        # For each target time, fetch measurements from all sensors
        for lag_key, target_time in target_times.items():
            aqi_values = []
            
            # Fetch from each sensor
            for sensor in sensors:
                sensor_id = sensor.get('id')
                if not sensor_id:
                    continue
                
                try:
                    headers = {"X-API-Key": get_next_api_key()}
                    # Fetch measurements around target time (±1 hour window)
                    date_from = (target_time - timedelta(hours=1)).isoformat()
                    date_to = (target_time + timedelta(hours=1)).isoformat()
                    
                    response = requests.get(
                        f"{BASE_URL}/sensors/{sensor_id}/measurements/hourly",
                        headers=headers,
                        params={
                            "datetime_from": date_from,
                            "datetime_to": date_to,
                            "limit": 10
                        },
                        timeout=5
                    )
                    
                    if response.status_code == 429:
                        headers["X-API-Key"] = get_next_api_key()
                        time.sleep(0.3)
                        continue
                    
                    if response.status_code != 200:
                        continue
                    
                    data = response.json()
                    results = data.get("results", [])
                    
                    # Find measurement closest to target time
                    for result in results:
                        try:
                            # Parse datetime from API response (same format as get_station_data)
                            dt_str = None
                            
                            # Try period.datetimeFrom.utc first (same as get_station_data)
                            period = result.get("period", {})
                            datetime_from = period.get("datetimeFrom", {})
                            if isinstance(datetime_from, dict):
                                dt_str = datetime_from.get("utc")
                            elif isinstance(datetime_from, str):
                                dt_str = datetime_from
                            
                            # Fallback to datetime.utc if period not available
                            if not dt_str:
                                dt_str = result.get("datetime", {}).get("utc")
                            
                            if not dt_str:
                                continue
                            
                            # Parse datetime string
                            if isinstance(dt_str, str):
                                if dt_str.endswith('Z'):
                                    dt_str = dt_str[:-1] + '+00:00'
                                dt = datetime.fromisoformat(dt_str)
                            else:
                                continue
                            
                            dt_hour = dt.replace(minute=0, second=0, microsecond=0)
                            
                            # Only use if matches target hour (allow ±1 hour tolerance for API timing)
                            time_diff = abs((dt_hour - target_time).total_seconds())
                            if time_diff <= 3600:  # Within 1 hour
                                parameter = result.get("parameter", {}).get("name", "").lower()
                                if not parameter:
                                    # Try alternative path
                                    param_info = result.get("parameter")
                                    if isinstance(param_info, dict):
                                        parameter = param_info.get("name", "").lower()
                                
                                value = result.get("value")
                                unit = result.get("unit", "")
                                
                                if value and parameter:
                                    value_standard = convert_unit_to_standard(value, parameter, unit)
                                    aqi = calculate_aqi_for_pollutant(value_standard, parameter)
                                    if aqi:
                                        aqi_values.append(aqi)
                        except Exception as e:
                            # Silently continue - some measurements may have different formats
                            continue
                    
                    time.sleep(0.1)  # Small delay between sensors
                    
                except requests.exceptions.RequestException:
                    continue
            
            # Take max AQI from all sensors for this time point
            if aqi_values:
                lag_aqi[lag_key] = max(aqi_values)
            else:
                lag_aqi[lag_key] = None
        
        return lag_aqi
        
    except Exception as e:
        print(f"Error in fetch_lag_features_from_openaq: {e}")
        return {}


def predict_aqi_next_hour(location_id, current_data, model_name='lightgbm'):
    """
    Predict AQI for next hour.
    
    Args:
        location_id: Location ID
        current_data: Current station data dict
        model_name: Model to use ('lightgbm' or 'xgboost')
    
    Returns:
        Predicted AQI value or None
    """
    # Load model
    model_data = get_model(model_name)
    if not model_data:
        return None
    
    model = model_data.get('model')
    feature_cols = model_data.get('feature_cols', [])
    
    if not model or not feature_cols:
        return None
    
    # Fetch only lag features from OpenAQ API (fast inference - no rolling statistics)
    # Get sensors from current_data if available
    sensors = current_data.get('sensors', [])
    if not sensors:
        # Try to get sensors from location endpoint
        try:
            headers = {"X-API-Key": get_next_api_key()}
            response = requests.get(
                f"{BASE_URL}/locations/{location_id}",
                headers=headers,
                timeout=5
            )
            if response.status_code == 200:
                data = response.json()
                location_data = data.get("results", [{}])[0]
                sensors = location_data.get("sensors", [])
        except:
            pass
    
    # Fetch only 3 lag features (1h, 24h, 168h ago) - fast API calls
    historical_data = None
    if sensors:
        historical_data = fetch_lag_features_from_openaq(location_id, sensors)
        # Debug: log lag feature values
        if historical_data:
            print(f"Debug - Lag features fetched: {historical_data}")
            fetched_lags = [k for k, v in historical_data.items() if v is not None]
            if fetched_lags:
                print(f"Fetched lag features: {fetched_lags}")
            else:
                print("No lag features fetched (all None)")
        else:
            print("No historical data fetched (empty dict)")
    else:
        print("No sensors available for lag feature fetching")
    
    # Create features
    try:
        features_df = create_features_for_prediction(current_data, historical_data)
        
        # Select only feature columns (in the same order as training)
        available_features = [f for f in feature_cols if f in features_df.columns]
        missing_features = [f for f in feature_cols if f not in features_df.columns]
        
        if missing_features:
            print(f"Warning: Missing features: {missing_features}")
            # Fill missing features with 0 (same as training)
            for feat in missing_features:
                features_df[feat] = 0
        
        # Ensure features are in the same order as training and convert to numeric
        X = features_df[feature_cols].copy()
        
        # Convert all columns to numeric (handle any string/object types)
        for col in X.columns:
            X[col] = pd.to_numeric(X[col], errors='coerce')
        
        # Fill NaN with 0 (same as training)
        X = X.fillna(0)
        
        # Debug: print feature values for first row
        print(f"Debug - Feature values (first 10): {dict(list(X.iloc[0].head(10).items()))}")
        print(f"Debug - AQI current: {current_data.get('aqi')}, lag_1h: {X['aqi_lag_1h'].iloc[0] if 'aqi_lag_1h' in X.columns else 'N/A'}")
        
        # Predict
        prediction = model.predict(X)[0]
        return max(0, float(prediction))  # Ensure non-negative
    except Exception as e:
        print(f"Error in prediction: {e}")
        import traceback
        traceback.print_exc()
        return None


@app.route('/')
def index():
    """Serve the main dashboard page."""
    return render_template('index.html', countries=COUNTRIES)


@app.route('/api/countries')
def get_countries():
    """Get list of available countries."""
    return jsonify({
        "countries": COUNTRIES,
        "success": True
    })


@app.route('/api/stations/<country_code>')
def get_stations(country_code):
    """
    Get list of stations for a specific country.
    Returns only station names and IDs for selection.
    """
    try:
        # Validate country code
        if country_code.upper() not in COUNTRIES:
            return jsonify({
                "success": False,
                "error": f"Invalid country code: {country_code}"
            }), 400

        country_code = country_code.upper()
        
        # Get locations from OpenAQ API with pagination to find alive stations
        headers = {"X-API-Key": get_next_api_key()}
        alive_threshold = datetime.now(timezone.utc) - timedelta(days=2)
        stations = []
        page = 1
        max_pages = 10  # Check up to 10 pages (1000 locations)
        
        while page <= max_pages and len(stations) < 50:  # Stop when we have 50 stations or checked enough pages
            params = {
                "iso": country_code,
                "limit": 100,
                "page": page
            }
            
            try:
                response = requests.get(
                    f"{BASE_URL}/locations",
                    headers=headers,
                    params=params,
                    timeout=10
                )
                
                if response.status_code == 429:
                    headers["X-API-Key"] = get_next_api_key()
                    time.sleep(0.5)
                    continue
                
                response.raise_for_status()
                data = response.json()
                results = data.get("results", [])
                
                if not results:
                    break
                
                # Filter stations that are alive (have data in last 2 days) and have relevant sensors
                for location in results:
                    # Check if station is alive (has recent data)
                    datetime_last = location.get("datetimeLast")
                    if not datetime_last or not datetime_last.get("utc"):
                        continue
                    
                    try:
                        last_update_str = datetime_last["utc"]
                        if last_update_str.endswith('Z'):
                            last_update_str = last_update_str[:-1] + '+00:00'
                        last_update = datetime.fromisoformat(last_update_str)
                        if last_update < alive_threshold:
                            continue  # Skip stations without recent data
                    except Exception as e:
                        continue
                    
                    # Check if has relevant sensors
                    sensors = location.get("sensors", [])
                    station_params = [s.get("parameter", {}).get("name", "").lower() for s in sensors]
                    
                    # Only include if has at least PM2.5 or PM10
                    has_relevant_sensor = any(p in station_params for p in ["pm25", "pm10"])
                    
                    if has_relevant_sensor:
                        stations.append({
                            "location_id": location.get("id"),
                            "location_name": location.get("name", "Unknown"),
                            "latitude": location.get("coordinates", {}).get("latitude", 0),
                            "longitude": location.get("coordinates", {}).get("longitude", 0)
                        })
                
                if len(results) < 100:
                    break  # Last page
                
                page += 1
                time.sleep(0.1)  # Small delay to avoid rate limiting
                
            except Exception as e:
                break
        
        return jsonify({
            "success": True,
            "country": {
                "code": country_code,
                "name": COUNTRIES[country_code]
            },
            "stations": stations
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Error fetching stations: {str(e)}"
        }), 500


@app.route('/api/station/<location_id>')
def get_station_data(location_id):
    """
    Get latest air quality data for a specific station.
    """
    try:
        # Get location details and measurements
        headers = {"X-API-Key": get_next_api_key()}
        
        # Get location details
        response = requests.get(
            f"{BASE_URL}/locations/{location_id}",
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 429:
            headers["X-API-Key"] = get_next_api_key()
            response = requests.get(
                f"{BASE_URL}/locations/{location_id}",
                headers=headers,
                timeout=10
            )
        
        if response.status_code == 404:
            return jsonify({
                "success": False,
                "error": "Station not found"
            }), 404
        
        response.raise_for_status()
        data = response.json()
        
        # Handle response structure: could be direct location or {"results": [location]}
        if "results" in data and isinstance(data["results"], list) and len(data["results"]) > 0:
            location = data["results"][0]
        else:
            location = data
        
        location_id = location.get("id")
        location_name = location.get("name", "Unknown")
        coordinates = location.get("coordinates", {})
        latitude = coordinates.get("latitude", 0)
        longitude = coordinates.get("longitude", 0)
        country_info = location.get("country", {})
        country_code = country_info.get("code", "")
        
        # Get sensors and their latest measurements
        sensors = location.get("sensors", [])
        station_params = {}
        max_aqi = 0
        max_aqi_category = "Good"
        last_updated = None
        
        date_to = datetime.now(timezone.utc)
        date_from = date_to - timedelta(hours=24)
        
        for sensor in sensors:
            param_info = sensor.get("parameter", {})
            param = param_info.get("name", "").lower()
            
            if param not in PARAMETERS:
                continue
            
            sensor_id = sensor.get("id")
            if not sensor_id:
                continue
            
            # Fetch latest measurement
            try:
                sensor_headers = {"X-API-Key": get_next_api_key()}
                sensor_response = requests.get(
                    f"{BASE_URL}/sensors/{sensor_id}/measurements/hourly",
                    headers=sensor_headers,
                    params={
                        "datetime_from": date_from.isoformat(),
                        "datetime_to": date_to.isoformat(),
                        "limit": 10,  # Get more to find the latest
                        "page": 1
                    },
                    timeout=5
                )
                
                if sensor_response.status_code == 200:
                    sensor_data = sensor_response.json()
                    measurements = sensor_data.get("results", [])
                    
                    if measurements:
                        # Get the most recent measurement (last in list)
                        latest_measurement = measurements[-1]
                        value = latest_measurement.get("value")
                        
                        # Check if this measurement is recent (within 2 days)
                        period = latest_measurement.get("period", {})
                        datetime_from = period.get("datetimeFrom", {})
                        measurement_time = None
                        if datetime_from and isinstance(datetime_from, dict) and datetime_from.get("utc"):
                            try:
                                time_str = datetime_from.get("utc")
                                if time_str.endswith('Z'):
                                    time_str = time_str[:-1] + '+00:00'
                                measurement_time = datetime.fromisoformat(time_str)
                            except:
                                pass
                        
                        # Only use if measurement is within 2 days
                        if measurement_time:
                            days_ago = (date_to - measurement_time).total_seconds() / 86400
                            if days_ago > 2:
                                continue  # Skip old measurements
                            # Update last_updated with measurement time
                            last_updated = measurement_time.isoformat()
                        
                        if value is not None:
                            unit = param_info.get("units", "")
                            
                            period = latest_measurement.get("period", {})
                            datetime_from = period.get("datetimeFrom", {})
                            if datetime_from and datetime_from.get("utc"):
                                last_updated = datetime_from.get("utc")
                            
                            value_standard = convert_unit_to_standard(value, param, unit)
                            aqi = calculate_aqi_for_pollutant(value_standard, param)
                            
                            if aqi:
                                station_params[param] = {
                                    "value": round(value, 2),
                                    "value_standard": round(value_standard, 2),
                                    "unit": unit,
                                    "aqi": aqi,
                                    "category": get_aqi_category(aqi)
                                }
                                
                                if aqi > max_aqi:
                                    max_aqi = aqi
                                    max_aqi_category = get_aqi_category(aqi)
                
                time.sleep(0.05)
                
            except Exception as e:
                continue
        
        if not station_params:
            return jsonify({
                "success": False,
                "error": "Không có dữ liệu measurements cho trạm này. Trạm có thể không có sensors hoạt động hoặc không có data trong 24 giờ gần nhất."
            }), 404
        
        overall_category = get_aqi_category(max_aqi)
        recommendation = get_recommendation(max_aqi, overall_category)
        
        # Prepare data for prediction
        parameters_list = list(station_params.keys())
        values_list = [station_params[p].get("value_standard", 0) for p in parameters_list]
        
        current_data = {
            "location_id": location_id,
            "location_name": location_name,
            "country": country_code,
            "latitude": latitude,
            "longitude": longitude,
            "aqi": max_aqi,
            "aqi_category": overall_category,
            "parameters": parameters_list,
            "values": values_list,
            "sensors": sensors  # Include sensors for lag feature fetching
        }
        
        # Predict AQI for next hour
        predicted_aqi = None
        try:
            predicted_aqi = predict_aqi_next_hour(location_id, current_data, model_name='lightgbm')
        except Exception as e:
            print(f"Prediction error: {e}")
        
        # Build response
        response_data = {
            "success": True,
            "station": {
                "location_id": location_id,
                "location_name": location_name,
                "latitude": latitude,
                "longitude": longitude,
                "country": country_code
            },
            "overall": {
                "aqi": max_aqi,
                "category": overall_category,
                "recommendation": recommendation
            },
            "parameters": station_params,
            "last_updated": last_updated or "",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Add prediction if available
        if predicted_aqi is not None:
            response_data["prediction"] = {
                "aqi_next_hour": round(predicted_aqi, 1),
                "category": get_aqi_category(predicted_aqi),
                "recommendation": get_recommendation(predicted_aqi, get_aqi_category(predicted_aqi))
            }
        
        return jsonify(response_data)
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Error fetching station data: {str(e)}"
        }), 500


@app.route('/api/latest/<country_code>')
def get_latest_data(country_code):
    """
    Get latest air quality data for a specific country.
    Aggregates data from all stations in the country.
    """
    try:
        # Validate country code
        if country_code.upper() not in COUNTRIES:
            return jsonify({
                "success": False,
                "error": f"Invalid country code: {country_code}"
            }), 400

        country_code = country_code.upper()
        
        # Get latest measurements from OpenAQ API
        headers = {"X-API-Key": get_next_api_key()}
        
        # Use locations endpoint to get locations with latest measurements
        # Limit to 50 locations to avoid too many API calls
        params = {
            "iso": country_code,
            "limit": 50
        }
        
        response = requests.get(
            f"{BASE_URL}/locations",
            headers=headers,
            params=params,
            timeout=10
        )
        
        if response.status_code == 429:
            # Rate limit, try with next API key
            headers["X-API-Key"] = get_next_api_key()
            response = requests.get(
                f"{BASE_URL}/locations",
                headers=headers,
                params=params,
                timeout=10
            )
        
        response.raise_for_status()
        data = response.json()
        
        results = data.get("results", [])
        
        if not results:
            return jsonify({
                "success": False,
                "error": f"No data available for {COUNTRIES[country_code]}"
            }), 404
        
        # Aggregate data from all stations
        aggregated = {
            "pm25": [],
            "pm10": [],
            "o3": [],
            "co": [],
            "so2": [],
            "no2": []
        }
        
        stations_data = []
        max_aqi = 0
        max_aqi_category = "Good"
        
        # Get latest measurements for each location
        date_to = datetime.now(timezone.utc)
        date_from = date_to - timedelta(hours=24)  # Last 24 hours to get latest data
        
        for location in results:
            location_id = location.get("id")
            location_name = location.get("name", "Unknown")
            coordinates = location.get("coordinates", {})
            latitude = coordinates.get("latitude", 0)
            longitude = coordinates.get("longitude", 0)
            
            # Get sensors from this location
            sensors = location.get("sensors", [])
            station_params = {}
            last_updated = None
            
            # Filter sensors to only those with parameters we need
            relevant_sensors = [s for s in sensors if s.get("parameter", {}).get("name", "").lower() in PARAMETERS]
            
            if not relevant_sensors:
                continue  # Skip locations without relevant sensors
            
            for sensor in relevant_sensors:
                param_info = sensor.get("parameter", {})
                param = param_info.get("name", "").lower()
                
                if param not in PARAMETERS:
                    continue
                
                sensor_id = sensor.get("id")
                if not sensor_id:
                    continue
                
                # Fetch latest measurement from sensor
                try:
                    sensor_headers = {"X-API-Key": get_next_api_key()}
                    sensor_response = requests.get(
                        f"{BASE_URL}/sensors/{sensor_id}/measurements/hourly",
                        headers=sensor_headers,
                        params={
                            "datetime_from": date_from.isoformat(),
                            "datetime_to": date_to.isoformat(),
                            "limit": 1,
                            "page": 1
                        },
                        timeout=5
                    )
                    
                    if sensor_response.status_code == 200:
                        sensor_data = sensor_response.json()
                        measurements = sensor_data.get("results", [])
                        
                        if measurements:
                            # Get the most recent measurement
                            latest_measurement = measurements[-1]
                            value = latest_measurement.get("value")
                            
                            if value is not None:
                                unit = param_info.get("units", "")
                                
                                # Get last updated time
                                period = latest_measurement.get("period", {})
                                datetime_from = period.get("datetimeFrom", {})
                                if datetime_from and datetime_from.get("utc"):
                                    last_updated = datetime_from.get("utc")
                                
                                # Convert to standard unit
                                value_standard = convert_unit_to_standard(value, param, unit)
                                # Calculate AQI
                                aqi = calculate_aqi_for_pollutant(value_standard, param)
                                
                                if aqi:
                                    aggregated[param].append({
                                        "value": value,
                                        "value_standard": value_standard,
                                        "unit": unit,
                                        "aqi": aqi
                                    })
                                    
                                    station_params[param] = {
                                        "value": round(value, 2),
                                        "value_standard": round(value_standard, 2),
                                        "unit": unit,
                                        "aqi": aqi,
                                        "category": get_aqi_category(aqi)
                                    }
                                    
                                    if aqi > max_aqi:
                                        max_aqi = aqi
                                        max_aqi_category = get_aqi_category(aqi)
                    
                    # Small delay to avoid rate limiting
                    time.sleep(0.05)
                    
                except Exception as e:
                    # Skip this sensor if there's an error
                    continue
            
            if station_params:
                stations_data.append({
                    "location_id": location_id,
                    "location_name": location_name,
                    "latitude": latitude,
                    "longitude": longitude,
                    "parameters": station_params,
                    "last_updated": last_updated or ""
                })
        
        # Calculate averages for country
        country_averages = {}
        country_max_aqi = {}
        
        for param in PARAMETERS:
            if aggregated[param]:
                values = [m["value_standard"] for m in aggregated[param]]
                aqis = [m["aqi"] for m in aggregated[param]]
                
                country_averages[param] = {
                    "avg_value": round(sum(values) / len(values), 2),
                    "max_value": round(max(values), 2),
                    "min_value": round(min(values), 2),
                    "unit": aggregated[param][0]["unit"],
                    "avg_aqi": round(sum(aqis) / len(aqis)),
                    "max_aqi": max(aqis),
                    "category": get_aqi_category(max(aqis)),
                    "station_count": len(aggregated[param])
                }
                country_max_aqi[param] = max(aqis)
        
        # Overall AQI is the maximum AQI across all parameters
        overall_aqi = max(country_max_aqi.values()) if country_max_aqi else 0
        overall_category = get_aqi_category(overall_aqi)
        recommendation = get_recommendation(overall_aqi, overall_category)
        
        return jsonify({
            "success": True,
            "country": {
                "code": country_code,
                "name": COUNTRIES[country_code]
            },
            "overall": {
                "aqi": overall_aqi,
                "category": overall_category,
                "recommendation": recommendation
            },
            "parameters": country_averages,
            "stations": stations_data,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        
    except requests.exceptions.RequestException as e:
        return jsonify({
            "success": False,
            "error": f"Error fetching data from OpenAQ API: {str(e)}"
        }), 500
    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Unexpected error: {str(e)}"
        }), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)

