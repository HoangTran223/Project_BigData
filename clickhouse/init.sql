-- Create database if not exists
CREATE DATABASE IF NOT EXISTS air_quality;

USE air_quality;

-- Bronze layer table (raw data from Kafka)
CREATE TABLE IF NOT EXISTS bronze_measurements
(
    datetime DateTime,
    location_id UInt64,
    location_name String,
    country String,
    latitude Float64,
    longitude Float64,
    parameter String,
    value Float64,
    value_standard Float64,
    unit String,
    aqi Float64,
    aqi_category String,
    ingestion_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (location_id, datetime, parameter);

-- Silver layer table (cleaned and normalized data)
CREATE TABLE IF NOT EXISTS silver_measurements
(
    datetime DateTime,
    location_id UInt64,
    location_name String,
    country String,
    latitude Float64,
    longitude Float64,
    parameter String,
    value Float64,
    unit String,
    aqi Float64,
    aqi_category String,
    processing_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (location_id, datetime, parameter);

-- Gold layer table (aggregated hourly AQI)
CREATE TABLE IF NOT EXISTS gold_hourly_aqi
(
    datetime DateTime,
    location_id UInt64,
    location_name String,
    country String,
    latitude Float64,
    longitude Float64,
    aqi Float64,
    aqi_category String,
    parameters Array(String),
    values Array(Float64),
    aggregation_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (location_id, datetime);

-- Create materialized view for real-time queries
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_aqi
ENGINE = MergeTree()
PARTITION BY country
ORDER BY (location_id, datetime)
AS SELECT
    datetime,
    location_id,
    location_name,
    country,
    latitude,
    longitude,
    aqi,
    aqi_category
FROM gold_hourly_aqi;

