# Hệ thống Thu thập và Xử lý Dữ liệu Chất lượng Không khí cho ML Training

## Mục đích

Hệ thống thu thập, xử lý và lưu trữ dữ liệu chất lượng không khí từ **OpenAQ API** cho khu vực **Đông Nam Á** (11 quốc gia), tính toán **AQI (Air Quality Index)** theo tiêu chuẩn US EPA, và lưu trữ trong Data Lake (Bronze/Silver/Gold) để phục vụ **ML Training** (time series prediction).

**Use Cases:**
- Thu thập historical data cho ML training
- Xử lý batch data theo pipeline tuần tự (giảm workload)
- Export dữ liệu theo quốc gia/date range cho training
- Phân tích time series data cho dự đoán chất lượng không khí

## Kiến trúc Hệ thống

```
OpenAQ API → Kafka → [Batch Pipeline] → MinIO (Bronze/Silver/Gold) → ClickHouse
                                    ↓
                            Kafka → Bronze → Silver → Gold
```

**Công nghệ:**
- **Kafka**: Message queue (buffer cho batch processing)
- **Spark Batch Processing**: Xử lý theo pipeline tuần tự
- **MinIO**: Object storage (S3-compatible, Parquet format)
- **ClickHouse**: OLAP database (phân tích và export)

## Quy trình Xử lý Dữ liệu

### Bước 1: Thu thập (Data Ingestion)
**Input:** OpenAQ API v3  
**Xử lý:**
- Lọc trạm còn hoạt động (có dữ liệu trong 7 ngày qua)
- Thu thập measurements theo giờ: PM2.5, PM10, O3, CO, SO2, NO2
- Chuyển đổi đơn vị về chuẩn US EPA
- Tính toán AQI và phân loại (Good, Moderate, Unhealthy, etc.)

**Output:** JSON messages → Kafka topic `openaq-raw-measurements`

**File:** `collect_data.py`

### Bước 2: Batch Processing (Spark)
**Input:** Kafka messages  
**Mục đích:** Xử lý dữ liệu theo pipeline tuần tự (Kafka → Bronze → Silver → Gold) để giảm workload và phù hợp với ML training

**Pipeline tuần tự (sequential):**

1. **Job 1: Kafka → Bronze** (Raw Data)
   - **Input:** Kafka topic `openaq-raw-measurements`
   - **Xử lý:** Parse JSON từ Kafka, lọc null, giữ nguyên 100% dữ liệu gốc
   - **Output:** `s3a://air-quality-data/bronze/year=YYYY/month=MM/day=DD/*.parquet`
   - **Workload:** Parse Kafka messages (1 lần duy nhất)

2. **Job 2: Bronze → Silver** (Cleaned Data)
   - **Input:** Parquet files từ Bronze (đọc từ MinIO, nhanh hơn Kafka)
   - **Xử lý:** 
     - Lọc hợp lệ (`value >= 0`, không null)
     - Chỉ lưu `value_standard` (đã convert về đơn vị chuẩn US EPA)
   - **Output:** `s3a://air-quality-data/silver/year=YYYY/month=MM/day=DD/*.parquet`
   - **Workload:** Đọc Parquet (nhanh hơn parse JSON từ Kafka)

3. **Job 3: Silver → Gold** (Aggregated by Hour)
   - **Input:** Parquet files từ Silver
   - **Xử lý:**
     - Aggregate theo `location_id` + `hour`
     - Tính AQI max, collect parameters và values thành arrays
   - **Output:** `s3a://air-quality-data/gold/year=YYYY/month=MM/day=DD/*.parquet`
   - **Workload:** Đọc Parquet và aggregate (không cần parse lại)

**Ưu điểm:**
- ✅ Giảm workload: Parse Kafka 1 lần (chỉ Bronze), Silver/Gold đọc từ Parquet
- ✅ Tái sử dụng data: Mỗi tầng phụ thuộc tầng trước, không parse lại
- ✅ Phù hợp batch processing: Chạy on-demand cho ML training

**File:** `spark/batch_processor.py`

### Bước 3: Load vào ClickHouse
**Input:** Parquet files từ MinIO (đã được Spark ghi ở Bước 2)  
**Mục đích:** Chuyển dữ liệu từ object storage (MinIO) sang OLAP database (ClickHouse) để query nhanh bằng SQL

**Luồng dữ liệu:**
- Bước 2: Spark ghi Parquet files → MinIO (`s3a://air-quality-data/bronze/`, `silver/`, `gold/`)
- Bước 3: Script Python đọc Parquet files từ MinIO → Insert vào ClickHouse tables

**Xử lý:** 
- Kết nối MinIO (S3-compatible) để đọc Parquet files
- Convert datetime columns
- Insert vào ClickHouse tables tương ứng

**Output:** 
- `bronze_measurements`: Dữ liệu thô (có thể query để replay)
- `silver_measurements`: Dữ liệu đã làm sạch (cho phân tích chi tiết)
- `gold_hourly_aqi`: AQI tổng hợp theo giờ (cho dashboard)
- `latest_aqi`: Materialized view tự động cập nhật (real-time queries)

**Tác dụng:** Cho phép query SQL nhanh với aggregation, filtering, grouping - không cần đọc từ Parquet files mỗi lần

**File:** `load_to_clickhouse.py`

### Bước 4: Phân tích
**Input:** SQL queries trên ClickHouse  
**Mục đích:** Truy vấn và phân tích dữ liệu để tạo dashboard, báo cáo, hoặc tích hợp với BI tools

**Cách dùng:**
```bash
docker exec -it clickhouse clickhouse-client --password default123

# Hoặc từ Python:
# client.query("SELECT ...")
```

**Ví dụ queries và kết quả:**

```sql
-- Xem AQI mới nhất
SELECT location_name, country, aqi, aqi_category, datetime 
FROM gold_hourly_aqi 
ORDER BY datetime DESC 
LIMIT 5;
```
**Kết quả:**
```
location_name           | country | aqi | aqi_category | datetime
Civil Engineering, NPIC | KH      | 17  | Good         | 2025-12-21 08:00:00
Kok Roka                | KH      | 39  | Good         | 2025-12-21 08:00:00
```

```sql
-- AQI trung bình theo quốc gia
SELECT country, avg(aqi) as avg_aqi, max(aqi) as max_aqi, count(*) as records
FROM gold_hourly_aqi 
GROUP BY country 
ORDER BY avg_aqi DESC;
```
**Kết quả:**
```
country | avg_aqi | max_aqi | records
KH      | 61.33   | 118     | 176
```

**Output:** Kết quả truy vấn dạng table có thể export CSV/JSON hoặc hiển thị trực tiếp trong dashboard

## Input/Output Tóm tắt

| Bước              | Input           | Output              | Technology                 |
|-------------------|-----------------|---------------------|----------------------------|
| **1. Ingestion**  | OpenAQ API      | JSON → Kafka        | Python, Kafka Producer     |
| **2. Processing** | Kafka messages  | Parquet → MinIO     | Spark Batch Processing     |
| **3. Loading**    | Parquet (MinIO) | Tables → ClickHouse | Python, clickhouse-connect |
| **4. Analytics**  | SQL queries     | Query results       | ClickHouse SQL             |

## Cấu trúc Dữ liệu

### Kafka Message Format
```json
{
    "datetime": "2024-01-15T10:00:00+00:00",
    "location_id": 12345,
    "location_name": "Station Name",
    "country": "VN",
    "latitude": 10.8231,
    "longitude": 106.6297,
    "parameter": "pm25",
    "value": 45.2,
    "value_standard": 45.2,
    "unit": "µg/m³",
    "aqi": 125,
    "aqi_category": "Unhealthy for Sensitive Groups",
    "ingestion_timestamp": "2024-01-15T10:05:23+00:00"
}
```

### MinIO Structure
```
air-quality-data/
├── bronze/year=YYYY/month=MM/day=DD/*.parquet
├── silver/year=YYYY/month=MM/day=DD/*.parquet
└── gold/year=YYYY/month=MM/day=DD/*.parquet
```

## Hướng dẫn Chạy

### 1. Setup
```bash
# Khởi động services
docker-compose up -d

# Setup MinIO bucket
python setup_minio_buckets.py
```

### 2. Thu thập Dữ liệu vào Kafka
```bash
python collect_data.py --mode historical --days 10000
```

### 3. Xử lý Batch Pipeline
```bash
# Start Spark container
docker-compose up -d spark-batch

# Chạy toàn bộ pipeline (Kafka → Bronze → Silver → Gold)
# Sử dụng spark-submit để tự động download dependencies
docker exec spark-batch spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app/spark/batch_processor.py --layer all

# Hoặc chạy từng bước riêng:
docker exec spark-batch spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app/spark/batch_processor.py --layer bronze  # Kafka → Bronze

docker exec spark-batch spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app/spark/batch_processor.py --layer silver  # Bronze → Silver

docker exec spark-batch spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app/spark/batch_processor.py --layer gold    # Silver → Gold
```

### 4. Load vào ClickHouse
```bash
python load_to_clickhouse.py --layer all
```

### 4. Truy vấn
```bash
docker exec -it clickhouse clickhouse-client

# Ví dụ queries:
SELECT * FROM gold_hourly_aqi ORDER BY datetime DESC LIMIT 10;
SELECT country, avg(aqi) FROM gold_hourly_aqi GROUP BY country;
```

## Ports

- **Kafka**: `localhost:9092`
- **MinIO API**: `http://localhost:9000`
- **MinIO Console**: `http://localhost:9001` (minioadmin/minioadmin123)
- **ClickHouse HTTP**: `localhost:8123`
- **ClickHouse Native**: `localhost:9002`


## Files Quan trọng

- `collect_data.py`: Thu thập dữ liệu từ OpenAQ → Kafka
- `spark/batch_processor.py`: Spark batch processing pipeline (Kafka → Bronze → Silver → Gold)
- `load_to_clickhouse.py`: Load từ MinIO → ClickHouse
- `aqi_calculator.py`: Tính toán AQI theo US EPA
- `docker-compose.yml`: Cấu hình tất cả services
- `clickhouse/init.sql`: Tạo ClickHouse tables

## Thu thập Dữ liệu Đầy đủ cho Training LLM

Để thu thập toàn bộ dữ liệu từ đầu tiên đến hiện tại từ 11 quốc gia Đông Nam Á phục vụ training LLM:

### Quy trình:

**Bước 1:** Thu thập dữ liệu vào Kafka
```bash
python collect_data.py --mode historical --days 10000
```

**Bước 2:** Xử lý Batch Pipeline
```bash
# Start Spark container
docker-compose up -d spark-batch

# Chạy toàn bộ pipeline (sử dụng spark-submit để tự động download dependencies)
docker exec spark-batch spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app/spark/batch_processor.py --layer all
```

**Bước 3:** Load vào ClickHouse
```bash
python load_to_clickhouse.py --layer all
```

### 4. Query và Export Dữ liệu theo Quốc gia cho Training
```bash
docker exec -it clickhouse clickhouse-client --password default123

# Xem số lượng records theo quốc gia (để verify dữ liệu đã thu thập)
SELECT country, count(*) as records, 
       min(datetime) as earliest_date, 
       max(datetime) as latest_date
FROM gold_hourly_aqi 
GROUP BY country 
ORDER BY records DESC;

# Export dữ liệu của từng quốc gia để training LLM
SELECT * FROM gold_hourly_aqi 
WHERE country = 'VN' 
ORDER BY datetime ASC 
INTO OUTFILE '/tmp/vietnam_aqi.csv' 
FORMAT CSV;

# Hoặc export từ Silver layer để có dữ liệu chi tiết hơn (theo từng parameter)
SELECT country, datetime, location_name, parameter, value, aqi, aqi_category
FROM silver_measurements
WHERE country = 'VN'
ORDER BY datetime ASC, location_id, parameter
INTO OUTFILE '/tmp/vietnam_silver.csv'
FORMAT CSV;

# Export tất cả quốc gia cùng lúc (cho training tổng hợp)
SELECT country, datetime, location_name, latitude, longitude, aqi, aqi_category, parameters, values
FROM gold_hourly_aqi
ORDER BY country, datetime ASC
INTO OUTFILE '/tmp/all_countries_aqi.csv'
FORMAT CSV;
```

**Dữ liệu sẽ được lưu trong:**
- **MinIO**: Parquet files (Bronze/Silver/Gold) được partition theo `year/month/day` và có field `country` - dùng cho Spark/ML training trực tiếp từ Parquet
- **ClickHouse**: Tables có thể query và export theo `country` - dùng cho phân tích và export CSV để training LLM

**Tips cho Training LLM:**
- Dữ liệu đã được phân theo `country` trong tất cả các layers
- Gold layer (`gold_hourly_aqi`) phù hợp cho time-series prediction (đã tổng hợp theo giờ)
- Silver layer (`silver_measurements`) phù hợp cho chi tiết từng parameter (PM2.5, PM10, O3, etc.)
