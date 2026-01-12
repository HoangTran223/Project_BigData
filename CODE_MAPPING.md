# Mapping Code Files với Kịch Bản Hệ Thống

File này mapping giữa các phần trong `openaq_kich_ban.md` với các file code tương ứng. Khi giáo viên hỏi "phần này code ở đâu", bạn có thể tra cứu nhanh.

---

## 📋 Mục lục

1. [Bước 1: Thu thập Dữ liệu (Data Ingestion)](#bước-1-thu-thập-dữ-liệu-data-ingestion)
2. [Bước 2: Batch Processing (Spark)](#bước-2-batch-processing-spark)
3. [Bước 3: Load vào ClickHouse](#bước-3-load-vào-clickhouse)
4. [Bước 4: Phân tích](#bước-4-phân-tích)
5. [Các Module Hỗ trợ](#các-module-hỗ-trợ)
6. [Cấu hình và Setup](#cấu-hình-và-setup)
7. [ML Training](#ml-training)
8. [Web Application (Inference)](#web-application-inference)

---

## Bước 1: Thu thập Dữ liệu (Data Ingestion)

### 📄 File chính: `collect_data.py`

**Nhiệm vụ:** Thu thập dữ liệu từ OpenAQ API và gửi vào Kafka

**Mapping với `openaq_kich_ban.md`:**

| Phần trong MD | Code Location | Mô tả |
|--------------|---------------|-------|
| **Xử lý tiền xử lý (Pre-processing)** - Lọc trạm còn hoạt động | `get_alive_stations()` (dòng 53-115) | Hàm lọc các trạm có dữ liệu trong 7 ngày gần nhất, có PM2.5 hoặc PM10 |
| **Quá trình thu thập** - Lấy danh sách trạm | `get_alive_stations()` (dòng 67-70) | Gọi API `/locations` với filter theo quốc gia |
| **Quá trình thu thập** - Lấy measurements | `fetch_measurements_for_sensor()` (dòng 118-177) | Gọi API `/sensors/{sensor_id}/measurements/hourly` để lấy dữ liệu lịch sử |
| **Xử lý dữ liệu** - Chuyển đổi đơn vị | `aqi_calculator.py` → `convert_unit_to_standard()` | Import và sử dụng từ module `aqi_calculator` |
| **Xử lý dữ liệu** - Tính toán AQI | `aqi_calculator.py` → `calculate_aqi_for_pollutant()` | Import và sử dụng từ module `aqi_calculator` |
| **Xử lý dữ liệu** - Phân loại AQI | `aqi_calculator.py` → `get_aqi_category()` | Import và sử dụng từ module `aqi_calculator` |
| **Output** - Gửi vào Kafka | `send_to_kafka()` (dòng 180-186) | Gửi JSON message vào topic `openaq-raw-measurements` |
| **Collect historical data** | `collect_historical_data()` (dòng 189-240) | Hàm chính để thu thập dữ liệu lịch sử |
| **Collect real-time data** | `collect_realtime_data()` (dòng 243-301) | Hàm để thu thập dữ liệu real-time (chạy liên tục) |
| **Entry point** | `main()` (dòng 304-365) | Hàm main, xử lý arguments, khởi tạo Kafka producer |

**Các hàm quan trọng:**
- `get_alive_stations()`: Lọc và trả về danh sách trạm còn hoạt động
- `fetch_measurements_for_sensor()`: Lấy measurements từ OpenAQ API
- `collect_historical_data()`: Thu thập dữ liệu lịch sử
- `collect_realtime_data()`: Thu thập dữ liệu real-time
- `send_to_kafka()`: Gửi message vào Kafka

**Cách chạy:**
```bash
python collect_data.py --mode historical --days 10000
```

---

## Bước 2: Batch Processing (Spark)

### 📄 File chính: `spark/batch_processor.py`

**Nhiệm vụ:** Xử lý dữ liệu từ Kafka → Bronze → Silver → Gold (pipeline tuần tự)

**Mapping với `openaq_kich_ban.md`:**

| Phần trong MD | Code Location | Mô tả |
|--------------|---------------|-------|
| **Bronze Layer** - Parse JSON | `process_kafka_to_bronze()` (dòng 80-133) | Đọc từ Kafka, parse JSON, extract fields |
| **Bronze Layer** - Filter cơ bản | `process_kafka_to_bronze()` (dòng 111-115) | Filter null cho `datetime`, `location_id`, `parameter` |
| **Bronze Layer** - Partition | `process_kafka_to_bronze()` (dòng 118-120) | Thêm columns `year`, `month`, `day` |
| **Bronze Layer** - Write to MinIO | `process_kafka_to_bronze()` (dòng 125-129) | Ghi Parquet files vào `s3a://air-quality-data/bronze/` |
| **Silver Layer** - Đọc từ Bronze | `process_bronze_to_silver()` (dòng 136-179) | Load Parquet files từ Bronze layer |
| **Silver Layer** - Data Cleaning | `process_bronze_to_silver()` (dòng 145-158) | Chỉ giữ `value_standard`, bỏ `value`, `unit`, `ingestion_timestamp` |
| **Silver Layer** - Validation Filters | `process_bronze_to_silver()` (dòng 159-166) | Filter null, value >= 0, không NaN |
| **Silver Layer** - Write to MinIO | `process_bronze_to_silver()` (dòng 171-175) | Ghi Parquet files vào `s3a://air-quality-data/silver/` |
| **Gold Layer** - Đọc từ Silver | `process_silver_to_gold()` (dòng 182-222) | Load Parquet files từ Silver layer |
| **Gold Layer** - Aggregation | `process_silver_to_gold()` (dòng 191-209) | Group by `location_id` + `hour_datetime`, aggregate AQI, parameters, values |
| **Gold Layer** - Write to MinIO | `process_silver_to_gold()` (dòng 214-218) | Ghi Parquet files vào `s3a://air-quality-data/gold/` |
| **Spark Session Setup** | `create_spark_session()` (dòng 48-77) | Cấu hình Spark với MinIO S3, adaptive execution |
| **Entry point** | `main()` (dòng 225-260) | Hàm main, xử lý arguments, gọi các hàm xử lý theo thứ tự |

**Các hàm quan trọng:**
- `create_spark_session()`: Tạo Spark session với cấu hình MinIO
- `process_kafka_to_bronze()`: Kafka → Bronze
- `process_bronze_to_silver()`: Bronze → Silver
- `process_silver_to_gold()`: Silver → Gold

**Cách chạy:**
```bash
docker exec spark-batch spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app/spark/batch_processor.py --layer all
```

---

## Bước 3: Load vào ClickHouse

### 📄 File chính: `load_to_clickhouse.py`

**Nhiệm vụ:** Đọc Parquet files từ MinIO và load vào ClickHouse tables

**Mapping với `openaq_kich_ban.md`:**

| Phần trong MD | Code Location | Mô tả |
|--------------|---------------|-------|
| **Kết nối MinIO** | `get_s3_filesystem()` (dòng 29-36) | Tạo S3 filesystem connection đến MinIO |
| **Kết nối ClickHouse** | `get_clickhouse_client()` (dòng 39-49) | Tạo ClickHouse client connection |
| **Load Bronze** | `load_bronze_to_clickhouse()` (dòng 52-105) | Đọc Parquet từ MinIO Bronze, insert vào table `bronze_measurements` |
| **Load Silver** | `load_silver_to_clickhouse()` (dòng 108-165) | Đọc Parquet từ MinIO Silver, insert vào table `silver_measurements` |
| **Load Gold** | `load_gold_to_clickhouse()` (dòng 168-236) | Đọc Parquet từ MinIO Gold, insert vào table `gold_hourly_aqi` |
| **Entry point** | `main()` (dòng 239-285) | Hàm main, xử lý arguments, gọi các hàm load theo layer |

**Các hàm quan trọng:**
- `get_s3_filesystem()`: Kết nối MinIO
- `get_clickhouse_client()`: Kết nối ClickHouse
- `load_bronze_to_clickhouse()`: Load Bronze layer
- `load_silver_to_clickhouse()`: Load Silver layer
- `load_gold_to_clickhouse()`: Load Gold layer

**Cách chạy:**
```bash
python load_to_clickhouse.py --layer all
```

---

## Bước 4: Phân tích

### 📄 File: `clickhouse/init.sql`

**Nhiệm vụ:** Tạo ClickHouse tables và materialized views

**Mapping với `openaq_kich_ban.md`:**

| Phần trong MD | Code Location | Mô tả |
|--------------|---------------|-------|
| **Tạo database** | `clickhouse/init.sql` (dòng đầu) | `CREATE DATABASE IF NOT EXISTS air_quality` |
| **Tạo table bronze_measurements** | `clickhouse/init.sql` | Schema cho Bronze layer |
| **Tạo table silver_measurements** | `clickhouse/init.sql` | Schema cho Silver layer |
| **Tạo table gold_hourly_aqi** | `clickhouse/init.sql` | Schema cho Gold layer |
| **Tạo materialized view latest_aqi** | `clickhouse/init.sql` | View tự động cập nhật cho real-time queries |

**File SQL queries mẫu:** `clickhouse_queries.sql`
- Chứa các query ví dụ để query ClickHouse
- Không phải file chạy tự động, chỉ là reference

---

## Các Module Hỗ trợ

### 📄 File: `aqi_calculator.py`

**Nhiệm vụ:** Tính toán AQI theo tiêu chuẩn US EPA

**Mapping với `openaq_kich_ban.md`:**

| Phần trong MD | Code Location | Mô tả |
|--------------|---------------|-------|
| **US EPA AQI Breakpoints** | `AQI_BREAKPOINTS` (dòng 8-56) | Dictionary chứa breakpoints cho từng pollutant |
| **Tính toán AQI** | `calculate_aqi_for_pollutant()` (dòng 59-88) | Hàm tính AQI bằng linear interpolation |
| **Chuyển đổi đơn vị** | `convert_unit_to_standard()` (dòng 91-112) | Convert về đơn vị chuẩn US EPA (ppm cho O3, CO, SO2, NO2) |
| **Phân loại AQI** | `get_aqi_category()` (dòng 115-128) | Trả về category: Good, Moderate, Unhealthy for Sensitive Groups, etc. |

**Được sử dụng bởi:**
- `collect_data.py`: Tính AQI khi thu thập dữ liệu
- `app.py`: Tính AQI trong inference pipeline

---

## Cấu hình và Setup

### 📄 File: `docker-compose.yml`

**Nhiệm vụ:** Cấu hình tất cả services (Kafka, MinIO, ClickHouse, Spark)

**Mapping với `openaq_kich_ban.md`:**

| Phần trong MD | Code Location | Mô tả |
|--------------|---------------|-------|
| **Zookeeper** | `zookeeper` service (dòng 3-13) | Service cho Kafka |
| **Kafka** | `kafka` service (dòng 16-35) | Message queue, port 9092 |
| **MinIO** | `minio` service (dòng 38-57) | S3-compatible storage, ports 9000 (API), 9001 (Console) |
| **ClickHouse** | `clickhouse` service (dòng 60-79) | OLAP database, ports 8123 (HTTP), 9002 (Native) |
| **Spark Batch** | `spark-batch` service (dòng 82-106) | Spark container cho batch processing |

### 📄 File: `setup_minio_buckets.py`

**Nhiệm vụ:** Tạo MinIO bucket `air-quality-data`

**Mapping với `openaq_kich_ban.md`:**

| Phần trong MD | Code Location | Mô tả |
|--------------|---------------|-------|
| **Setup MinIO bucket** | `setup_minio()` (dòng 15-40) | Tạo bucket nếu chưa tồn tại |

**Cách chạy:**
```bash
python setup_minio_buckets.py
```

### 📄 File: `Dockerfile.spark`

**Nhiệm vụ:** Build Docker image cho Spark container

**Mapping với `openaq_kich_ban.md`:**

| Phần trong MD | Code Location | Mô tả |
|--------------|---------------|-------|
| **Spark container setup** | Toàn bộ file | Cài đặt Java 11, Python 3.11, PySpark, dependencies |

---

## ML Training

### 📄 File: `ml_training/data_loader.py`

**Nhiệm vụ:** Load dữ liệu từ MinIO Gold layer cho ML training

**Mapping với `openaq_kich_ban.md`:**

| Phần trong MD | Code Location | Mô tả |
|--------------|---------------|-------|
| **Load Gold layer** | `load_gold_layer()` (dòng 49-152) | Đọc Parquet files từ MinIO Gold layer |
| **Feature Engineering** | `create_features()` (dòng 154-252) | Tạo 22 features: time, lag, rolling statistics, spatial, pollutant |
| **Train/Test Split** | `split_train_val_test()` (dòng 254-280) | Split theo thời gian: Train (≤2023), Validation (2024), Test (>2024) |

**Class chính:**
- `AirQualityDataLoader`: Class để load và preprocess dữ liệu

### 📄 File: `ml_training/train_lightgbm.py`

**Nhiệm vụ:** Training model LightGBM

**Mapping với `openaq_kich_ban.md`:**

| Phần trong MD | Code Location | Mô tả |
|--------------|---------------|-------|
| **Training LightGBM** | Toàn bộ file | Load data, create features, train LightGBM model, save model |

### 📄 File: `ml_training/train_xgboost.py`

**Nhiệm vụ:** Training model XGBoost

**Mapping với `openaq_kich_ban.md`:**

| Phần trong MD | Code Location | Mô tả |
|--------------|---------------|-------|
| **Training XGBoost** | Toàn bộ file | Load data, create features, train XGBoost model, save model |

### 📄 File: `ml_training/evaluate.py`

**Nhiệm vụ:** Đánh giá model performance

**Mapping với `openaq_kich_ban.md`:**

| Phần trong MD | Code Location | Mô tả |
|--------------|---------------|-------|
| **Model evaluation** | Toàn bộ file | Load model, evaluate trên test set, tính metrics (R², MAE, RMSE) |

### 📄 File: `ml_training/check_countries.py`

**Nhiệm vụ:** Kiểm tra thống kê dữ liệu trong Gold layer

**Mapping với `openaq_kich_ban.md`:**

| Phần trong MD | Code Location | Mô tả |
|--------------|---------------|-------|
| **Thống kê dữ liệu** | Toàn bộ file | Đếm records, ước lượng size, phân tích 5V Big Data, thống kê theo quốc gia |

**Cách chạy:**
```bash
python ml_training/check_countries.py
```

---

## Web Application (Inference)

### 📄 File: `app.py`

**Nhiệm vụ:** Flask web application cho inference (dự đoán AQI real-time)

**Mapping với `openaq_kich_ban.md`:**

| Phần trong MD | Code Location | Mô tả |
|--------------|---------------|-------|
| **Inference Pipeline** - Fetch Current Data | `get_station_data()` (dòng ~700-830) | Gọi OpenAQ API `/locations/{location_id}` và `/sensors/{sensor_id}/measurements/hourly` |
| **Inference Pipeline** - Fetch Historical Data | `get_station_data()` (dòng ~700-830) | Gọi API với `datetime_from` = 1h, 24h, 168h trước để lấy lag features |
| **Inference Pipeline** - Feature Engineering | `create_features_for_prediction()` (dòng ~400-600) | Tạo 22 features từ current và historical data |
| **Inference Pipeline** - Prediction | `get_station_data()` (dòng ~800-820) | Load model, predict `aqi_next`, trả về kết quả |
| **API Endpoint** - `/api/station/<location_id>` | `get_station_data()` (dòng ~650-830) | Endpoint để lấy thông tin trạm và prediction |
| **API Endpoint** - `/api/latest/<country_code>` | `get_latest_data()` (dòng 832-1000) | Endpoint để lấy dữ liệu mới nhất theo quốc gia |
| **Frontend** | `templates/` và `static/` | HTML, CSS, JavaScript cho dashboard |

**Các hàm quan trọng:**
- `create_features_for_prediction()`: Tạo features cho inference (22 features)
- `get_station_data()`: Endpoint chính cho inference
- `get_latest_data()`: Endpoint lấy dữ liệu mới nhất theo quốc gia
- `get_recommendation()`: Tạo recommendation message dựa trên AQI

**Cách chạy:**
```bash
python app.py
# Hoặc
./run_frontend.sh
```

---

## Tóm tắt Mapping nhanh

| Chức năng | File chính | Hàm/Class chính |
|-----------|-----------|-----------------|
| **Thu thập dữ liệu** | `collect_data.py` | `get_alive_stations()`, `collect_historical_data()` |
| **Tính AQI** | `aqi_calculator.py` | `calculate_aqi_for_pollutant()`, `convert_unit_to_standard()` |
| **Bronze Layer** | `spark/batch_processor.py` | `process_kafka_to_bronze()` |
| **Silver Layer** | `spark/batch_processor.py` | `process_bronze_to_silver()` |
| **Gold Layer** | `spark/batch_processor.py` | `process_silver_to_gold()` |
| **Load ClickHouse** | `load_to_clickhouse.py` | `load_bronze_to_clickhouse()`, `load_silver_to_clickhouse()`, `load_gold_to_clickhouse()` |
| **ML Training** | `ml_training/data_loader.py` | `AirQualityDataLoader`, `create_features()` |
| **Inference** | `app.py` | `create_features_for_prediction()`, `get_station_data()` |
| **Setup** | `setup_minio_buckets.py` | `setup_minio()` |
| **Docker** | `docker-compose.yml` | Cấu hình services |

---

## Các file khác

### 📄 File: `requirements.txt`
- Danh sách Python dependencies

### 📄 File: `cleanup_all_data.sh`
- Script để xóa dữ liệu trong MinIO và ClickHouse (testing/cleanup)

### 📄 File: `run_frontend.sh`
- Script để chạy Flask app

### 📄 File: `SLIDE_PREPARATION_GUIDE.md`
- Hướng dẫn chuẩn bị slide presentation (không phải code)

### 📄 File: `openaq_kich_ban.md`
- Kịch bản hệ thống (documentation, không phải code)

---

**Lưu ý:** File này được tạo để hỗ trợ trả lời câu hỏi "phần này code ở đâu" khi giáo viên hỏi. Bạn có thể tra cứu nhanh bằng cách tìm phần tương ứng trong `openaq_kich_ban.md` và xem mapping ở trên.

