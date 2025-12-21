"""
Script to load data from MinIO (S3) to ClickHouse.
This script reads Parquet files from MinIO and loads them into ClickHouse tables.
"""

import clickhouse_connect
import s3fs
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime
import argparse
import os
from tqdm import tqdm

# Configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "air-quality-data")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "air_quality")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "default123")
# Use native protocol port if HTTP fails
CLICKHOUSE_NATIVE_PORT = int(os.environ.get("CLICKHOUSE_NATIVE_PORT", "9002"))


def get_s3_filesystem():
    """Create S3 filesystem connection to MinIO."""
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        use_ssl=False
    )


def get_clickhouse_client():
    """Create ClickHouse client connection."""
    # clickhouse-connect requires username and password parameters
    # For empty password, pass empty string explicitly
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DATABASE,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD  # Empty string if no password
    )


def load_bronze_to_clickhouse(client, fs, date_filter=None):
    """Load Bronze layer data from MinIO to ClickHouse."""
    print("Loading Bronze layer data...")
    
    bronze_path = f"{MINIO_BUCKET}/bronze/"
    
    # List all parquet files
    if date_filter:
        bronze_path = f"{bronze_path}year={date_filter.year}/month={date_filter.month}/day={date_filter.day}/"
    
    parquet_files = fs.glob(f"{bronze_path}**/*.parquet")
    
    if not parquet_files:
        print(f"No Bronze files found in {bronze_path}")
        return
    
    print(f"Found {len(parquet_files)} Parquet files")
    
    total_records = 0
    for file_path in tqdm(parquet_files, desc="Loading Bronze files"):
        try:
            # Read Parquet file
            with fs.open(file_path, 'rb') as f:
                table = pq.read_table(f)
                df = table.to_pandas()
            
            if len(df) == 0:
                continue
            
            # Convert datetime columns
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['ingestion_timestamp'] = pd.to_datetime(df['ingestion_timestamp'], errors='coerce')
            
            # Insert into ClickHouse
            client.insert_df('bronze_measurements', df)
            total_records += len(df)
            
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
    
    print(f"Loaded {total_records} records to Bronze table")


def load_silver_to_clickhouse(client, fs, date_filter=None):
    """Load Silver layer data from MinIO to ClickHouse."""
    print("Loading Silver layer data...")
    
    silver_path = f"{MINIO_BUCKET}/silver/"
    
    if date_filter:
        silver_path = f"{silver_path}year={date_filter.year}/month={date_filter.month}/day={date_filter.day}/"
    
    parquet_files = fs.glob(f"{silver_path}**/*.parquet")
    
    if not parquet_files:
        print(f"No Silver files found in {silver_path}")
        return
    
    print(f"Found {len(parquet_files)} Parquet files")
    
    total_records = 0
    for file_path in tqdm(parquet_files, desc="Loading Silver files"):
        try:
            with fs.open(file_path, 'rb') as f:
                table = pq.read_table(f)
                df = table.to_pandas()
            
            if len(df) == 0:
                continue
            
            # Convert datetime columns
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['processing_timestamp'] = pd.to_datetime(df['processing_timestamp'], errors='coerce')
            
            # Insert into ClickHouse
            client.insert_df('silver_measurements', df)
            total_records += len(df)
            
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
    
    print(f"Loaded {total_records} records to Silver table")


def load_gold_to_clickhouse(client, fs, date_filter=None):
    """Load Gold layer data from MinIO to ClickHouse."""
    print("Loading Gold layer data...")
    
    gold_path = f"{MINIO_BUCKET}/gold/"
    
    if date_filter:
        gold_path = f"{gold_path}year={date_filter.year}/month={date_filter.month}/day={date_filter.day}/"
    
    parquet_files = fs.glob(f"{gold_path}**/*.parquet")
    
    if not parquet_files:
        print(f"No Gold files found in {gold_path}")
        return
    
    print(f"Found {len(parquet_files)} Parquet files")
    
    total_records = 0
    for file_path in tqdm(parquet_files, desc="Loading Gold files"):
        try:
            with fs.open(file_path, 'rb') as f:
                table = pq.read_table(f)
                df = table.to_pandas()
            
            if len(df) == 0:
                continue
            
            # Convert datetime columns
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['aggregation_timestamp'] = pd.to_datetime(df['aggregation_timestamp'], errors='coerce')
            
            # Ensure arrays are proper lists for ClickHouse
            # Parquet files from Spark should already have arrays as lists
            if 'parameters' in df.columns:
                df['parameters'] = df['parameters'].apply(
                    lambda x: x if isinstance(x, list) else (list(x) if hasattr(x, '__iter__') and not isinstance(x, str) else [])
                )
            if 'values' in df.columns:
                df['values'] = df['values'].apply(
                    lambda x: x if isinstance(x, list) else (list(x) if hasattr(x, '__iter__') and not isinstance(x, str) else [])
                )
            
            # Insert into ClickHouse
            client.insert_df('gold_hourly_aqi', df)
            total_records += len(df)
            
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
    
    print(f"Loaded {total_records} records to Gold table")


def main():
    parser = argparse.ArgumentParser(description='Load data from MinIO to ClickHouse')
    parser.add_argument('--layer', type=str, choices=['bronze', 'silver', 'gold', 'all'], 
                        default='all', help='Which layer to load (default: all)')
    parser.add_argument('--date', type=str, default=None,
                        help='Filter by date (YYYY-MM-DD format)')
    
    args = parser.parse_args()
    
    print("="*80)
    print("MinIO to ClickHouse Data Loader")
    print("="*80)
    print(f"MinIO endpoint: {MINIO_ENDPOINT}")
    print(f"ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
    print(f"Layer: {args.layer}")
    print("="*80)
    
    # Parse date filter if provided
    date_filter = None
    if args.date:
        date_filter = datetime.strptime(args.date, "%Y-%m-%d")
        print(f"Date filter: {date_filter.date()}")
    
    # Connect to services
    try:
        fs = get_s3_filesystem()
        client = get_clickhouse_client()
        print("Connected to MinIO and ClickHouse successfully")
    except Exception as e:
        print(f"Error connecting to services: {e}")
        return
    
    # Load data based on layer
    if args.layer in ['bronze', 'all']:
        load_bronze_to_clickhouse(client, fs, date_filter)
    
    if args.layer in ['silver', 'all']:
        load_silver_to_clickhouse(client, fs, date_filter)
    
    if args.layer in ['gold', 'all']:
        load_gold_to_clickhouse(client, fs, date_filter)
    
    print("\nData loading complete!")


if __name__ == "__main__":
    main()

