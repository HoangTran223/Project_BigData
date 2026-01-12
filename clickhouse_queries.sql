-- ClickHouse Queries for Presentation Slides
-- Run these queries in ClickHouse client or HTTP interface

-- ============================================================================
-- 1. DATA STATISTICS
-- ============================================================================

-- Bronze layer statistics
SELECT 
    count(*) as total_records,
    min(datetime) as earliest_date,
    max(datetime) as latest_date,
    count(DISTINCT location_id) as unique_locations,
    count(DISTINCT country) as unique_countries
FROM bronze_measurements;

-- Silver layer statistics
SELECT 
    count(*) as total_records,
    min(datetime) as earliest_date,
    max(datetime) as latest_date,
    count(DISTINCT location_id) as unique_locations
FROM silver_measurements;

-- Gold layer statistics
SELECT 
    count(*) as total_records,
    min(datetime) as earliest_date,
    max(datetime) as latest_date,
    count(DISTINCT location_id) as unique_locations,
    count(DISTINCT country) as unique_countries
FROM gold_hourly_aqi;

-- ============================================================================
-- 2. SAMPLE RECORDS
-- ============================================================================

-- Sample from Bronze
SELECT *
FROM bronze_measurements
LIMIT 1
FORMAT JSON;

-- Sample from Silver
SELECT *
FROM silver_measurements
LIMIT 1
FORMAT JSON;

-- Sample from Gold
SELECT *
FROM gold_hourly_aqi
LIMIT 1
FORMAT JSON;

-- ============================================================================
-- 3. ALIVE STATIONS (Vietnam example)
-- ============================================================================

-- List stations in Vietnam
SELECT DISTINCT 
    location_id,
    location_name,
    country,
    count(*) as record_count,
    min(datetime) as first_measurement,
    max(datetime) as last_measurement
FROM gold_hourly_aqi
WHERE country = 'VN'
GROUP BY location_id, location_name, country
ORDER BY location_name
LIMIT 20;

-- ============================================================================
-- 4. LATEST AQI DATA
-- ============================================================================

-- Latest AQI (Top 10)
SELECT 
    location_name,
    country,
    aqi,
    aqi_category,
    datetime
FROM gold_hourly_aqi
ORDER BY datetime DESC
LIMIT 10
FORMAT Pretty;

-- ============================================================================
-- 5. COUNTRY STATISTICS
-- ============================================================================

-- AQI Statistics by Country
SELECT 
    country,
    count(*) as records,
    avg(aqi) as avg_aqi,
    max(aqi) as max_aqi,
    min(aqi) as min_aqi,
    stddevPop(aqi) as std_aqi
FROM gold_hourly_aqi
GROUP BY country
ORDER BY avg_aqi DESC
FORMAT Pretty;

-- ============================================================================
-- 6. DATA QUALITY CHECK
-- ============================================================================

-- Check for null values in Bronze
SELECT 
    count(*) as total,
    countIf(datetime IS NULL) as null_datetime,
    countIf(location_id IS NULL) as null_location,
    countIf(parameter IS NULL) as null_parameter,
    countIf(value IS NULL) as null_value
FROM bronze_measurements;

-- Check for invalid values (negative)
SELECT 
    count(*) as total,
    countIf(value < 0) as negative_values,
    countIf(value_standard < 0) as negative_standard
FROM bronze_measurements;

-- ============================================================================
-- 7. TIME RANGE ANALYSIS
-- ============================================================================

-- Records per year
SELECT 
    toYear(datetime) as year,
    count(*) as records
FROM gold_hourly_aqi
GROUP BY year
ORDER BY year;

-- Records per country per year
SELECT 
    country,
    toYear(datetime) as year,
    count(*) as records
FROM gold_hourly_aqi
GROUP BY country, year
ORDER BY country, year;

-- ============================================================================
-- 8. SENSOR/PARAMETER STATISTICS
-- ============================================================================

-- Parameters distribution in Bronze
SELECT 
    parameter,
    count(*) as count,
    avg(value_standard) as avg_value,
    max(value_standard) as max_value
FROM bronze_measurements
GROUP BY parameter
ORDER BY count DESC;

-- ============================================================================
-- 9. EXPORT FOR PRESENTATION
-- ============================================================================

-- Export sample records to CSV (for slides)
SELECT *
FROM bronze_measurements
LIMIT 5
INTO OUTFILE '/tmp/bronze_sample.csv'
FORMAT CSVWithNames;

SELECT *
FROM silver_measurements
LIMIT 5
INTO OUTFILE '/tmp/silver_sample.csv'
FORMAT CSVWithNames;

SELECT *
FROM gold_hourly_aqi
LIMIT 5
INTO OUTFILE '/tmp/gold_sample.csv'
FORMAT CSVWithNames;

