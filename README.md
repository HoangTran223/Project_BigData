# Hệ thống Phân tích và Giám sát Chất lượng Không khí Real-time

## Mục đích

Hệ thống thu thập, xử lý và phân tích dữ liệu chất lượng không khí từ **OpenAQ API** cho khu vực **Đông Nam Á** (11 quốc gia), tính toán **AQI (Air Quality Index)** theo tiêu chuẩn US EPA, và lưu trữ trong Data Lake (Bronze/Silver/Gold) để phân tích.

**Use Cases:**
- Giám sát chất lượng không khí real-time
- Phân tích xu hướng ô nhiễm theo thời gian và địa lý
- So sánh chất lượng không khí giữa các quốc gia/trạm
- Cảnh báo khi AQI vượt ngưỡng an toàn

## Kiến trúc Hệ thống

```
OpenAQ API → Kafka → Spark Streaming → MinIO (Bronze/Silver/Gold) → ClickHouse
```

**Công nghệ:**
- **Kafka**: Message queue (streaming buffer)
- **Spark Structured Streaming**: Xử lý real-time
- **MinIO**: Object storage (S3-compatible, Parquet format)
- **ClickHouse**: OLAP database (phân tích nhanh)

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

### Bước 2: Stream Processing (Spark)
**Input:** Kafka messages  
**Mục đích:** Xử lý dữ liệu theo 3 tầng Data Lake để phục vụ các use case khác nhau

**Xử lý 3 tầng song song:**

1. **Bronze** (Raw): Lưu toàn bộ dữ liệu thô từ Kafka
   - **Khác biệt:** Giữ nguyên 100% dữ liệu gốc, không filter, có cả `value` (gốc) và `value_standard` (đã convert)
   - **Ví dụ:** Một trạm có 10 measurements trong 1 giờ → lưu cả 10 records
   - Trigger: 10 giây | Format: Parquet | Partition: `year=YYYY/month=MM/day=DD/`

2. **Silver** (Cleaned): Lọc và chuẩn hóa dữ liệu
   - **Khác biệt:** 
     - Chỉ giữ dữ liệu hợp lệ (value >= 0, không null)
     - Chuẩn hóa datetime
     - Chỉ lưu `value_standard` (giá trị đã convert về đơn vị chuẩn US EPA: PM2.5/PM10 = µg/m³, O3/CO/SO2/NO2 = ppm)
   - **Ví dụ:** 
     - Input: `{"value": 45.2, "unit": "µg/m³", "value_standard": 45.2}` (PM2.5) → Output: `{"value": 45.2}` (PM2.5 đã là µg/m³ nên không cần convert)
     - Input: `{"value": 120, "unit": "µg/m³", "value_standard": 0.055}` (O3) → Output: `{"value": 0.055}` (O3 convert từ 120 µg/m³ → 0.055 ppm theo công thức US EPA)
     - **Giải thích:** O3 từ API có thể là µg/m³ nhưng US EPA tính AQI theo ppm, nên phải convert. `value: 120` là giá trị gốc (có thể khác nhau giữa các trạm), `value_standard: 0.055` là giá trị chuẩn để tính AQI
   - Trigger: 10 giây | Format: Parquet | Partition: `year=YYYY/month=MM/day=DD/`

3. **Gold** (Aggregated): Tổng hợp theo giờ (AQI)
   - **Khác biệt:** 
     - Nhóm tất cả measurements của cùng 1 location trong cùng 1 giờ thành 1 record
     - Tính AQI cao nhất (max) trong giờ đó
     - Tổng hợp tất cả parameters thành arrays: `parameters: ["pm25", "pm10", "o3"]`, `values: [45.2, 60.1, 0.055]`
   - **Ví dụ:** 
     - Input: 1 trạm có 3 measurements trong giờ 10:00 (pm25 AQI=50, pm10 AQI=60, o3 AQI=45)
     - Output: 1 record với `aqi: 60` (max), `parameters: ["pm25", "pm10", "o3"]`, `values: [50, 60, 45]`
   - Trigger: 1 phút | Format: Parquet | Partition: `year=YYYY/month=MM/day=DD/`

**Output:** 
- Spark ghi trực tiếp Parquet files vào **MinIO** (object storage S3-compatible)
- Location: `s3a://air-quality-data/` bucket trong MinIO
- Mỗi tầng có thư mục riêng: `bronze/`, `silver/`, `gold/`
- Files được partition theo ngày: `year=2025/month=12/day=21/*.parquet`

**File:** `spark/stream_processor.py`

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
| **2. Processing** | Kafka messages  | Parquet → MinIO     | Spark Structured Streaming |
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

# Khởi động Spark streaming
docker-compose up -d --build spark-streaming
```

### 2. Thu thập Dữ liệu
```bash
# Đợi Spark khởi động xong (30-60 giây), sau đó:
python collect_data.py --mode historical --days 7 --sample 5
```

**⚠️ Lưu ý:** Spark chỉ đọc dữ liệu mới từ khi khởi động (`startingOffsets: "latest"`). Phải gửi dữ liệu SAU KHI Spark đã chạy.

### 3. Load vào ClickHouse
```bash
# Đợi Spark xử lý (10-60 giây), sau đó:
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
- `spark/stream_processor.py`: Spark streaming job (Kafka → MinIO)
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
- Đợi script xong = đã gửi hết dữ liệu vào Kafka
- Spark sẽ tiếp tục xử lý dữ liệu từ Kafka và ghi vào MinIO

**Bước 2:** Đợi Spark xử lý xong
```bash
# Kiểm tra số lượng files trong MinIO (chạy nhiều lần để xem có tăng không)
python3 -c "import s3fs; fs = s3fs.S3FileSystem(key='minioadmin', secret='minioadmin123', endpoint_url='http://localhost:9000', use_ssl=False); print('Bronze:', len(fs.glob('air-quality-data/bronze/**/*.parquet'))); print('Silver:', len(fs.glob('air-quality-data/silver/**/*.parquet'))); print('Gold:', len(fs.glob('air-quality-data/gold/**/*.parquet')))"
```
- Spark xử lý xong khi: Số lượng files không tăng thêm nữa (sau khi Bước 1 đã xong)

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
