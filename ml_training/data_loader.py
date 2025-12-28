"""
Data Loader for ML Training
Loads Gold layer data from MinIO (Parquet) and prepares for training.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import s3fs
import pyarrow.parquet as pq
from typing import Optional, Tuple
import os
from sklearn.preprocessing import LabelEncoder


class AirQualityDataLoader:
    """Load and preprocess air quality data for ML training from MinIO."""
    
    def __init__(self, minio_endpoint: str = None):
        """
        Initialize data loader.
        
        Args:
            minio_endpoint: MinIO endpoint URL (auto-detect if None)
        """
        # Auto-detect endpoint: try minio (Docker) first, then localhost
        if minio_endpoint is None:
            import socket
            # Try to connect to minio:9000 (Docker network)
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex(('minio', 9000))
                sock.close()
                if result == 0:
                    minio_endpoint = "http://minio:9000"
                else:
                    minio_endpoint = "http://localhost:9000"
            except:
                minio_endpoint = "http://localhost:9000"
        
        self.minio_endpoint = minio_endpoint
        print(f"Connecting to MinIO at: {minio_endpoint}")
        
        self.fs = s3fs.S3FileSystem(
            key='minioadmin',
            secret='minioadmin123',
            endpoint_url=minio_endpoint,
            use_ssl=False
        )
        self.country_encoder = LabelEncoder()
    
    def load_gold_layer(self, 
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       countries: Optional[list] = None,
                       sample_frac: Optional[float] = None) -> pd.DataFrame:
        """
        Load Gold layer data (hourly aggregated AQI) from MinIO.
        
        Args:
            start_date: Start date (YYYY-MM-DD), default: all
            end_date: End date (YYYY-MM-DD), default: all
            countries: List of country codes to filter, default: all
            sample_frac: Fraction to sample (for testing), default: None (all data)
        
        Returns:
            DataFrame with columns: datetime, location_id, country, aqi, etc.
        """
        """Load from MinIO Parquet files."""
        bucket = 'air-quality-data'
        gold_path = f'{bucket}/gold/'
        
        # Get all parquet files
        files = sorted(self.fs.glob(f'{gold_path}**/*.parquet'))
        
        if not files:
            raise ValueError("No parquet files found in MinIO gold layer")
        
        print(f"Found {len(files)} parquet files. Loading...")
        
        # Load files with progress tracking
        dfs = []
        for i, file_path in enumerate(files):
            if sample_frac and np.random.random() > sample_frac:
                continue
            
            try:
                with self.fs.open(file_path, 'rb') as f:
                    df = pq.read_table(f).to_pandas()
                    dfs.append(df)
            except Exception as e:
                print(f"Warning: Error loading {file_path}: {e}")
                continue
            
            if (i + 1) % 100 == 0:
                print(f"Loaded {i + 1}/{len(files)} files...")
        
        if not dfs:
            raise ValueError("No data loaded from parquet files")
        
        df = pd.concat(dfs, ignore_index=True)
        
        # Convert datetime
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Filter by date
        if start_date:
            df = df[df['datetime'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['datetime'] <= pd.to_datetime(end_date)]
        
        # Filter by country
        if countries:
            df = df[df['country'].isin(countries)]
        
        # Sort by datetime and location (critical for lag features)
        df = df.sort_values(['location_id', 'datetime']).reset_index(drop=True)
        
        print(f"Loaded {len(df)} records from {df['datetime'].min()} to {df['datetime'].max()}")
        print(f"Countries: {sorted(df['country'].unique())}")
        print(f"Locations: {df['location_id'].nunique()} unique locations")
        
        return df
    
    def create_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create features for ML training.
        
        Args:
            df: Raw DataFrame from load_gold_layer()
        
        Returns:
            DataFrame with additional features
        """
        df = df.copy()
        
        print("Creating time features...")
        # Time features
        df['hour'] = df['datetime'].dt.hour
        df['day_of_week'] = df['datetime'].dt.dayofweek
        df['month'] = df['datetime'].dt.month
        df['day_of_year'] = df['datetime'].dt.dayofyear
        df['is_weekend'] = df['datetime'].dt.dayofweek >= 5
        
        print("Creating lag features...")
        # Lag features (AQI from previous hours) - groupby location_id to preserve time series
        df['aqi_lag_1h'] = df.groupby('location_id')['aqi'].shift(1)
        df['aqi_lag_24h'] = df.groupby('location_id')['aqi'].shift(24)
        df['aqi_lag_168h'] = df.groupby('location_id')['aqi'].shift(168)  # 1 week
        
        print("Creating rolling statistics...")
        # Rolling statistics (7 days = 168 hours)
        df['aqi_mean_7d'] = df.groupby('location_id')['aqi'].transform(
            lambda x: x.rolling(168, min_periods=1).mean()
        )
        df['aqi_std_7d'] = df.groupby('location_id')['aqi'].transform(
            lambda x: x.rolling(168, min_periods=1).std()
        )
        df['aqi_max_7d'] = df.groupby('location_id')['aqi'].transform(
            lambda x: x.rolling(168, min_periods=1).max()
        )
        df['aqi_min_7d'] = df.groupby('location_id')['aqi'].transform(
            lambda x: x.rolling(168, min_periods=1).min()
        )
        
        # Rolling statistics (30 days = 720 hours)
        df['aqi_mean_30d'] = df.groupby('location_id')['aqi'].transform(
            lambda x: x.rolling(720, min_periods=1).mean()
        )
        
        print("Encoding country...")
        # Country encoding
        df['country_encoded'] = self.country_encoder.fit_transform(df['country'])
        
        print("Extracting pollutant values from arrays...")
        # Extract pollutant values from arrays
        pollutants = ['pm25', 'pm10', 'o3', 'co', 'so2', 'no2']
        for pollutant in pollutants:
            df[pollutant] = df.apply(
                lambda row: self._extract_pollutant_value(row, pollutant), 
                axis=1
            )
        
        # Fill NaN for pollutants (if not available)
        for pollutant in pollutants:
            df[pollutant] = df[pollutant].fillna(0.0)
        
        return df
    
    def _extract_pollutant_value(self, row, pollutant: str) -> Optional[float]:
        """Extract pollutant value from parameters and values arrays."""
        try:
            if pd.isna(row.get('parameters')) or pd.isna(row.get('values')):
                return None
            
            params = row['parameters']
            values = row['values']
            
            # Handle list/array types
            if isinstance(params, list) and isinstance(values, list):
                if pollutant in params:
                    idx = params.index(pollutant)
                    return float(values[idx])
        except (ValueError, IndexError, TypeError):
            pass
        
        return None
    
    def split_train_val_test(self, 
                            df: pd.DataFrame,
                            train_end: str = "2023-12-31",
                            val_end: str = "2024-12-31") -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Split data into train/validation/test sets (time-based).
        
        Args:
            df: DataFrame with datetime column
            train_end: End date for training set (YYYY-MM-DD)
            val_end: End date for validation set (YYYY-MM-DD)
        
        Returns:
            train_df, val_df, test_df
        """
        df = df.copy()
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        train_df = df[df['datetime'] <= pd.to_datetime(train_end)]
        val_df = df[
            (df['datetime'] > pd.to_datetime(train_end)) &
            (df['datetime'] <= pd.to_datetime(val_end))
        ]
        test_df = df[df['datetime'] > pd.to_datetime(val_end)]
        
        print(f"Train: {len(train_df)} records ({train_df['datetime'].min()} to {train_df['datetime'].max()})")
        print(f"Val:   {len(val_df)} records ({val_df['datetime'].min()} to {val_df['datetime'].max()})")
        print(f"Test:  {len(test_df)} records ({test_df['datetime'].min()} to {test_df['datetime'].max()})")
        
        return train_df, val_df, test_df


def main():
    """Example usage."""
    loader = AirQualityDataLoader()
    
    # Load data
    df = loader.load_gold_layer(
        start_date="2020-01-01",
        end_date="2024-12-31"
    )
    
    # Create features
    df_features = loader.create_features(df)
    
    # Split data
    train_df, val_df, test_df = loader.split_train_val_test(df_features)
    
    print("\nFeature columns:", list(df_features.columns))
    print("\nSample data:")
    print(df_features.head())


if __name__ == "__main__":
    main()

