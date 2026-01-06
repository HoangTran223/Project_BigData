# Hệ thống Thu thập và Xử lý Dữ liệu Chất lượng Không khí

## Mục tiêu

Xây dựng một hệ thống lưu trữ và xử lý dữ liệu lớn dựa trên kiến trúc Data Lake (Kafka-based). Hệ thống thu thập dữ liệu chất lượng không khí (PM2.5, PM10, CO, NO₂, SO₂, O₃) từ **OpenAQ API** cho khu vực **Đông Nam Á** (9 quốc gia), tính toán **AQI (Air Quality Index)** theo tiêu chuẩn US EPA, và lưu trữ trong Data Lake (Bronze/Silver/Gold) để phục vụ **ML Training** và phân tích.

**Use Cases:**
- Thu thập historical data cho ML training
- Xử lý batch data theo pipeline tuần tự (giảm workload)
- Export dữ liệu theo quốc gia/date range cho training
- Phân tích time series data cho dự đoán chất lượng không khí
- Query và phân tích dữ liệu qua ClickHouse

## Kiến trúc Hệ thống

```
OpenAQ API → Kafka → [Batch Pipeline] → MinIO (Bronze/Silver/Gold) → ClickHouse
                                    ↓
                            Kafka → Bronze → Silver → Gold
```

**Công nghệ:**
- **Python + OpenAQ REST API**: Thu thập dữ liệu từ các endpoint `/locations`, `/measurements`
- **Apache Kafka**: Message queue (buffer cho batch processing)
- **Apache Spark Batch Processing**: Xử lý theo pipeline tuần tự (Kafka → Bronze → Silver → Gold)
- **MinIO (S3-compatible)**: Object storage, Parquet format, partition theo `year/month/day`
- **ClickHouse**: OLAP database (phân tích nhanh, query SQL, export dữ liệu)

## Quy trình Xử lý Dữ liệu

### Bước 1: Thu thập (Data Ingestion)
**Input:** OpenAQ API v3  
**File:** `collect_data.py`

**Xử lý tiền xử lý (Pre-processing):**

Trước khi thu thập dữ liệu, hệ thống lọc các trạm đo còn hoạt động (active stations):
- Chỉ lấy các trạm có dữ liệu trong vòng 7 ngày gần nhất
- Kiểm tra `datetimeLast` của mỗi trạm để xác định trạm còn hoạt động
- Chỉ giữ lại trạm có ít nhất một trong hai sensor: PM2.5 hoặc PM10 (các chỉ số quan trọng nhất)

**Kết quả lọc:**
- Từ 11 quốc gia Đông Nam Á (BN, KH, ID, LA, MY, MM, PH, SG, TH, TL, VN)
- Chỉ thu thập được từ 9 quốc gia có trạm còn sống: ID, KH, LA, MM, MY, PH, SG, TH, VN
- Thiếu BN (Brunei) và TL (Timor-Leste) do không có trạm thỏa điều kiện
- Tổng cộng: **542 trạm** còn hoạt động

**Quá trình thu thập:**
1. Lấy danh sách trạm: Gọi API `/locations` với filter theo từng quốc gia
2. Lấy measurements: Với mỗi trạm, gọi API `/measurements` để lấy dữ liệu lịch sử
3. Xử lý dữ liệu:
   - Chuyển đổi đơn vị về chuẩn US EPA (ví dụ: ppm → µg/m³)
   - Tính toán AQI (Air Quality Index) theo tiêu chuẩn US EPA
   - Phân loại AQI thành các mức: Good, Moderate, Unhealthy for Sensitive Groups, Unhealthy, Very Unhealthy, Hazardous

**Output:** JSON messages → Kafka topic `openaq-raw-measurements`

**Số lượng dữ liệu thu thập:**
- Tổng số records trong Kafka: **~11 triệu records** (mỗi record = 1 parameter tại 1 thời điểm)
- Thời gian: Từ 2016-01-30 đến 2025-12-24 (tùy theo quốc gia)
- Các parameters: PM2.5, PM10, O3, CO, SO2, NO2

### Bước 2: Batch Processing (Spark)
**Input:** Kafka messages  
**Mục đích:** Xử lý dữ liệu theo pipeline tuần tự (Kafka → Bronze → Silver → Gold) để giảm workload và phù hợp với ML training

**Pipeline tuần tự (sequential):**

#### 1. **Bronze Layer** (Raw Data - Lưu trữ dữ liệu gốc)

**File:** `spark/batch_processor.py`  
**Hàm:** `process_kafka_to_bronze(spark)`

**Input:** Kafka topic `openaq-raw-measurements` (JSON messages)

**Xử lý:**
- **Parse JSON**: Chuyển đổi JSON messages từ Kafka thành Spark DataFrame
- **Extract fields**: Trích xuất tất cả fields từ schema:
  - `datetime`, `location_id`, `location_name`, `country`
  - `latitude`, `longitude`
  - `parameter` (pm25, pm10, o3, co, so2, no2)
  - `value` (giá trị gốc từ sensor)
  - `value_standard` (giá trị đã convert về đơn vị chuẩn US EPA)
  - `unit` (đơn vị gốc: µg/m³, ppm, etc.)
  - `aqi`, `aqi_category`
  - `ingestion_timestamp` (thời điểm thu thập vào Kafka)
- **Filter cơ bản**: Chỉ loại bỏ records có `datetime`, `location_id`, hoặc `parameter` = null
- **Partition**: Thêm columns `year`, `month`, `day` để partition theo ngày

**Output:** 
- Path: `s3a://air-quality-data/bronze/year=YYYY/month=MM/day=DD/*.parquet`
- Format: Parquet (columnar, compressed)
- **Đặc điểm**: Giữ nguyên 100% dữ liệu gốc, không mất thông tin

**Mục đích:**
- ✅ Lưu trữ raw data để có thể replay/reprocess nếu cần
- ✅ Audit trail: Giữ lại `ingestion_timestamp` và `unit` gốc
- ✅ Data lineage: Có thể trace lại nguồn gốc dữ liệu

**Ví dụ record:**
```
datetime: 2024-01-15 10:00:00
location_id: 12345
parameter: pm25
value: 45.2
value_standard: 45.2
unit: µg/m³
aqi: 125
aqi_category: "Unhealthy for Sensitive Groups"
```

---

#### 2. **Silver Layer** (Cleaned Data - Dữ liệu đã làm sạch)

**File:** `spark/batch_processor.py`  
**Hàm:** `process_bronze_to_silver(spark)`

**Input:** Parquet files từ Bronze layer (đọc từ MinIO, nhanh hơn parse JSON từ Kafka)

**Xử lý:**
- **Đọc từ Bronze**: Load Parquet files từ MinIO (nhanh hơn parse JSON)
- **Data Cleaning**:
  - Chỉ giữ `value_standard` (đã chuẩn hóa về US EPA), bỏ `value` gốc
  - Loại bỏ `unit` (không cần thiết vì đã chuẩn hóa)
  - Loại bỏ `ingestion_timestamp` (không cần cho ML training)
- **Validation Filters**:
  - `datetime` không null
  - `location_id` không null
  - `parameter` không null
  - `value_standard` không null
  - `value_standard >= 0` (chỉ giá trị hợp lệ)
  - `value_standard` không phải NaN
- **Giữ lại**: `datetime`, `location_id`, `location_name`, `country`, `latitude`, `longitude`, `parameter`, `value` (từ value_standard), `aqi`, `aqi_category`

**Output:**
- Path: `s3a://air-quality-data/silver/year=YYYY/month=MM/day=DD/*.parquet`
- Format: Parquet (columnar, compressed)
- **Đặc điểm**: Dữ liệu đã được làm sạch, chuẩn hóa, sẵn sàng cho phân tích

**Mục đích:**
- ✅ Data quality: Loại bỏ invalid values, outliers cơ bản
- ✅ Standardization: Chỉ giữ giá trị đã chuẩn hóa (US EPA)
- ✅ Simplified schema: Bỏ các fields không cần thiết cho ML

**Ví dụ record:**
```
datetime: 2024-01-15 10:00:00
location_id: 12345
parameter: pm25
value: 45.2  (đã chuẩn hóa, không còn unit)
aqi: 125
aqi_category: "Unhealthy for Sensitive Groups"
```

**So với Bronze:**
- ❌ Bỏ: `value` (gốc), `unit`, `ingestion_timestamp`
- ✅ Giữ: Tất cả fields còn lại, nhưng `value` = `value_standard` từ Bronze

---

#### 3. **Gold Layer** (Aggregated Data - Dữ liệu tổng hợp theo giờ)

**File:** `spark/batch_processor.py`  
**Hàm:** `process_silver_to_gold(spark)`

**Input:** Parquet files từ Silver layer

**Xử lý:**
- **Đọc từ Silver**: Load Parquet files từ MinIO
- **Aggregation**:
  - Group by: `location_id` + `hour_datetime` (truncate datetime về giờ)
  - **AQI**: Lấy `max(aqi)` trong giờ đó (worst case)
  - **Parameters**: `collect_list(parameter)` → array các parameters có trong giờ đó
  - **Values**: `collect_list(value)` → array các giá trị tương ứng với parameters
  - **AQI Category**: Lấy `max(aqi_category)` (worst case)
- **Giữ lại**: `datetime` (hour level), `location_id`, `location_name`, `country`, `latitude`, `longitude`, `aqi`, `aqi_category`, `parameters` (array), `values` (array)

**Output:**
- Path: `s3a://air-quality-data/gold/year=YYYY/month=MM/day=DD/*.parquet`
- Format: Parquet (columnar, compressed)
- **Đặc điểm**: 1 record = 1 location × 1 giờ, với tất cả parameters trong arrays

**Mục đích:**
- ✅ Time series format: Phù hợp cho ML training (hourly data)
- ✅ Aggregated: Giảm số lượng records (từ ~11M → ~5.4M)
- ✅ Complete picture: Mỗi record chứa tất cả pollutants trong giờ đó

**Ví dụ record:**
```
datetime: 2024-01-15 10:00:00
location_id: 12345
location_name: "Station Name"
country: "VN"
latitude: 10.8231
longitude: 106.6297
aqi: 125  (max trong giờ)
aqi_category: "Unhealthy for Sensitive Groups"
parameters: ["pm25", "pm10", "o3"]  (array)
values: [45.2, 78.5, 120.3]  (array, tương ứng với parameters)
```

**So với Silver:**
- ✅ Aggregated: Nhiều records (1 record/parameter/giờ) → 1 record/location/giờ
- ✅ Arrays: `parameters` và `values` là arrays thay vì separate columns
- ✅ Simplified: Không còn column `parameter` riêng lẻ

**Lưu ý:**
- Số records giảm từ ~11M (Bronze/Silver) xuống ~5.4M (Gold) là bình thường
- Lý do: Nhiều parameters trong cùng 1 giờ được aggregate thành 1 record

---

**Ưu điểm của Pipeline tuần tự:**
- ✅ **Giảm workload**: Parse Kafka chỉ 1 lần (Bronze), Silver/Gold đọc từ Parquet (nhanh hơn)
- ✅ **Tái sử dụng data**: Mỗi tầng phụ thuộc tầng trước, không parse lại từ Kafka
- ✅ **Phù hợp batch processing**: Chạy on-demand cho ML training, không cần real-time
- ✅ **Data quality**: Từng bước làm sạch và chuẩn hóa dữ liệu
- ✅ **Flexibility**: Có thể chạy lại từng tầng riêng lẻ nếu cần

**Bảng so sánh các tầng:**

| Aspect | Bronze | Silver | Gold |
|--------|--------|--------|------|
| **File** | `spark/batch_processor.py` | `spark/batch_processor.py` | `spark/batch_processor.py` |
| **Hàm** | `process_kafka_to_bronze()` | `process_bronze_to_silver()` | `process_silver_to_gold()` |
| **Input** | Kafka (JSON) | Bronze Parquet | Silver Parquet |
| **Records** | ~11M | ~11M | ~5.4M |
| **Format** | 1 record/parameter/time | 1 record/parameter/time | 1 record/location/hour |
| **Fields** | Tất cả (bao gồm `value`, `unit`, `ingestion_timestamp`) | Đã cleaned (chỉ `value_standard`) | Aggregated (arrays) |
| **Data Quality** | Raw (chỉ filter null cơ bản) | Cleaned (validate values) | Aggregated (max AQI) |
| **Use Case** | Audit, replay | Analysis, detailed queries | ML training, time series |
| **Schema** | Flat (separate columns) | Flat (separate columns) | Nested (arrays) |

**File chính:** `spark/batch_processor.py`  
**Entry point:** `main()` - gọi các hàm xử lý theo thứ tự tuần tự

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

## Export Dữ liệu theo Quốc gia cho Training
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

**Lưu ý:**
- **MinIO**: Parquet files (Bronze/Silver/Gold) partition theo `year/month/day`, có field `country` - dùng cho Spark/ML training trực tiếp
- **ClickHouse**: Tables có thể query và export theo `country` - dùng cho phân tích và export CSV
- **Gold layer** (`gold_hourly_aqi`): Phù hợp cho time-series prediction (đã tổng hợp theo giờ)
- **Silver layer** (`silver_measurements`): Phù hợp cho chi tiết từng parameter (PM2.5, PM10, O3, etc.)
