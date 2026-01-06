# ML Training Pipeline - Air Quality Prediction

## Mục tiêu

Xây dựng mô hình Machine Learning để dự đoán **AQI (Air Quality Index) cho 1 giờ tiếp theo** dựa trên dữ liệu chất lượng không khí hiện tại và lịch sử. Model được train trên dữ liệu từ 9 quốc gia Đông Nam Á (ID, KH, LA, MM, MY, PH, SG, TH, VN).

**Model Repository**: [HoangTran223/BigData_ML_Training](https://github.com/HoangTran223/BigData_ML_Training)

---

## Input Data - Cấu trúc Dữ liệu

### Nguồn Dữ liệu

Dữ liệu được lấy từ **MinIO Gold Layer** (đã được xử lý và tổng hợp theo giờ):
- **Format**: Parquet files (columnar, compressed)
- **Path**: `s3a://air-quality-data/gold/year=YYYY/month=MM/day=DD/*.parquet`
- **Time Range**: 2016-01-30 đến 2025-12-24
- **Countries**: 9 quốc gia (ID, KH, LA, MM, MY, PH, SG, TH, VN)

### Cấu trúc Dữ liệu Gốc (Gold Layer)

Mỗi record trong Gold layer đại diện cho **1 trạm đo tại 1 giờ cụ thể**, chứa các thông tin sau:

**Metadata:**
- `datetime`: Thời điểm đo (timestamp, ví dụ: 2024-01-15 10:00:00)
- `location_id`: ID duy nhất của trạm đo (số nguyên)
- `location_name`: Tên trạm đo (chuỗi, ví dụ: "Hanoi Station")
- `country`: Mã quốc gia (2 ký tự, ví dụ: "VN", "TH")
- `latitude`: Vĩ độ của trạm đo (số thực, ví dụ: 10.8231)
- `longitude`: Kinh độ của trạm đo (số thực, ví dụ: 106.6297)

**Chất lượng Không khí:**
- `aqi`: Chỉ số AQI tại thời điểm đó (số thực, ví dụ: 125.5)
- `aqi_category`: Phân loại AQI (chuỗi, ví dụ: "Unhealthy for Sensitive Groups")
- `parameters`: Mảng các thông số đã đo trong giờ đó (ví dụ: ["pm25", "pm10", "o3"])
- `values`: Mảng giá trị tương ứng với các thông số (ví dụ: [45.2, 78.5, 120.3])

**Ví dụ một record thực tế:**
```
datetime: 2024-01-15 10:00:00
location_id: 12345
location_name: "Hanoi Station"
country: "VN"
latitude: 21.0285
longitude: 105.8542
aqi: 125
aqi_category: "Unhealthy for Sensitive Groups"
parameters: ["pm25", "pm10", "o3", "no2"]
values: [45.2, 78.5, 120.3, 51.9]
```

---

## Feature Engineering - Các Features Model Học

Từ dữ liệu gốc, model tạo ra **22 features** để học patterns. Các features được chia thành các nhóm sau:

### 1. Time Features (5 features)

Các đặc trưng về thời gian được trích xuất từ `datetime`:
- **hour**: Giờ trong ngày (0-23) - giúp model nhận biết patterns theo giờ (ví dụ: buổi sáng thường có AQI cao hơn)
- **day_of_week**: Thứ trong tuần (0=Thứ 2, 6=Chủ nhật) - nhận biết patterns theo ngày trong tuần
- **month**: Tháng trong năm (1-12) - nhận biết mùa (ví dụ: mùa khô thường có AQI cao hơn)
- **day_of_year**: Ngày thứ bao nhiêu trong năm (1-365) - nhận biết chu kỳ theo năm
- **is_weekend**: Có phải cuối tuần không (True/False) - nhận biết sự khác biệt giữa ngày làm việc và cuối tuần

**Ví dụ:** Với `datetime = 2024-01-15 10:00:00` (Thứ 2, 10 giờ sáng):
- `hour = 10`
- `day_of_week = 0` (Thứ 2)
- `month = 1` (Tháng 1)
- `day_of_year = 15`
- `is_weekend = False`

### 2. Lag Features (3 features)

Các giá trị AQI từ quá khứ, giúp model nhận biết xu hướng:
- **aqi_lag_1h**: AQI của 1 giờ trước (ví dụ: tại 10:00, lấy AQI lúc 9:00)
- **aqi_lag_24h**: AQI của 24 giờ trước (ví dụ: tại 10:00, lấy AQI lúc 10:00 hôm qua)
- **aqi_lag_168h**: AQI của 168 giờ trước = 1 tuần trước (ví dụ: tại 10:00, lấy AQI lúc 10:00 tuần trước)

**Ví dụ:** Nếu tại 10:00 hôm nay AQI = 125, và:
- 9:00 hôm nay AQI = 120 → `aqi_lag_1h = 120`
- 10:00 hôm qua AQI = 110 → `aqi_lag_24h = 110`
- 10:00 tuần trước AQI = 105 → `aqi_lag_168h = 105`

### 3. Rolling Statistics Features (5 features)

Các thống kê trượt (rolling statistics) của AQI trong khoảng thời gian gần đây:
- **aqi_mean_7d**: Trung bình AQI trong 7 ngày qua (168 giờ)
- **aqi_std_7d**: Độ lệch chuẩn AQI trong 7 ngày qua (đo độ biến động)
- **aqi_max_7d**: AQI cao nhất trong 7 ngày qua
- **aqi_min_7d**: AQI thấp nhất trong 7 ngày qua
- **aqi_mean_30d**: Trung bình AQI trong 30 ngày qua (720 giờ)

**Ví dụ:** Nếu trong 7 ngày qua, AQI dao động từ 80 đến 150:
- `aqi_mean_7d = 115` (trung bình)
- `aqi_std_7d = 20` (độ lệch chuẩn)
- `aqi_max_7d = 150` (cao nhất)
- `aqi_min_7d = 80` (thấp nhất)

**Lưu ý khi không có historical data (inference):**
Khi dự đoán với dữ liệu mới mà không có historical data (ví dụ: trạm đo mới, hoặc không load được từ MinIO), các rolling statistics sẽ được fill bằng giá trị mặc định:
- `aqi_mean_7d = current_aqi` (dùng AQI hiện tại)
- `aqi_std_7d = 0` (không có độ lệch)
- `aqi_max_7d = current_aqi` (dùng AQI hiện tại)
- `aqi_min_7d = current_aqi` (dùng AQI hiện tại)
- `aqi_mean_30d = current_aqi` (dùng AQI hiện tại)

Điều này đảm bảo model vẫn có thể predict được, nhưng độ chính xác sẽ thấp hơn so với khi có đầy đủ historical data.

### 4. Spatial Features (3 features)

Các đặc trưng về vị trí địa lý:
- **country_encoded**: Mã hóa quốc gia thành số (ví dụ: VN = 0, TH = 1, ...) - giúp model nhận biết patterns theo quốc gia
- **latitude**: Vĩ độ (ví dụ: 21.0285) - giúp model nhận biết vùng khí hậu
- **longitude**: Kinh độ (ví dụ: 105.8542) - giúp model nhận biết vùng địa lý

**Ví dụ:** Với trạm ở Hà Nội:
- `country_encoded = 8` (giả sử VN được encode thành 8)
- `latitude = 21.0285`
- `longitude = 105.8542`

### 5. Pollutant Features (6 features)

Các giá trị cụ thể của từng chất ô nhiễm, được trích xuất từ mảng `parameters` và `values`:
- **pm25**: Nồng độ PM2.5 (µg/m³) - bụi mịn 2.5 micromet
- **pm10**: Nồng độ PM10 (µg/m³) - bụi mịn 10 micromet
- **o3**: Nồng độ Ozone (µg/m³)
- **co**: Nồng độ Carbon Monoxide (µg/m³)
- **so2**: Nồng độ Sulfur Dioxide (µg/m³)
- **no2**: Nồng độ Nitrogen Dioxide (µg/m³)

**Ví dụ:** Với `parameters = ["pm25", "pm10", "o3"]` và `values = [45.2, 78.5, 120.3]`:
- `pm25 = 45.2`
- `pm10 = 78.5`
- `o3 = 120.3`
- `co = 0.0` (không có trong parameters, fill bằng 0)
- `so2 = 0.0`
- `no2 = 0.0`

### 6. Target Variable

- **aqi_next**: AQI của **1 giờ tiếp theo** - đây là giá trị model cần dự đoán

**Ví dụ:** Nếu tại 10:00, AQI = 125, và tại 11:00, AQI = 130, thì:
- Record tại 10:00 có `aqi = 125` và `aqi_next = 130`
- Model học: từ features tại 10:00 → dự đoán AQI tại 11:00 = 130

---

## Ví dụ Dữ liệu Truyền cho Model

Dưới đây là một ví dụ cụ thể về một record sau khi đã được feature engineering, sẵn sàng để train hoặc predict:

**Input record gốc:**
```
datetime: 2024-01-15 10:00:00
location_id: 12345
location_name: "Hanoi Station"
country: "VN"
latitude: 21.0285
longitude: 105.8542
aqi: 125
aqi_category: "Unhealthy for Sensitive Groups"
parameters: ["pm25", "pm10", "o3", "no2"]
values: [45.2, 78.5, 120.3, 51.9]
```

**Sau feature engineering, model nhận được vector features:**
```
hour: 10
day_of_week: 0
month: 1
day_of_year: 15
is_weekend: 0 (False)
aqi_lag_1h: 120.0
aqi_lag_24h: 110.0
aqi_lag_168h: 105.0
aqi_mean_7d: 115.5
aqi_std_7d: 18.2
aqi_max_7d: 150.0
aqi_min_7d: 80.0
aqi_mean_30d: 108.3
country_encoded: 8
latitude: 21.0285
longitude: 105.8542
pm25: 45.2
pm10: 78.5
o3: 120.3
co: 0.0
so2: 0.0
no2: 51.9
```

**Target:**
```
aqi_next: 130.0 (AQI tại 11:00)
```

Model sẽ học: từ vector features trên → dự đoán `aqi_next = 130.0`.

---

## Training Pipeline

### Setup

```bash
# 1. Start MinIO (nếu chưa chạy)
cd /home/user/Project
docker-compose up -d minio

# 2. Install dependencies
pip install -r requirements_ml.txt
```

### Training

LightGBM và XGBoost sử dụng cùng một pipeline training:

```bash
cd /home/user/Project/ml_training

# Train với full data (recommended)
python train_lightgbm.py  # Hoặc train_xgboost.py

# Train với sample data (for testing)
python train_lightgbm.py --sample 0.1

# Train với custom date range
python train_lightgbm.py --start-date 2020-01-01 --end-date 2024-12-31
```

### Evaluate Model

```bash
python evaluate.py --model models/lightgbm_global.pkl
```

**Quy trình:**
1. Load data từ MinIO Gold Layer (Parquet format, hourly aggregated)
2. Feature engineering (tạo tất cả 22 features như mô tả ở trên)
3. Tạo target `aqi_next` (shift AQI 1 giờ về trước)
4. Split data: Train (≤2023), Validation (2024), Test (>2024)
5. Train model với early stopping
6. Evaluate trên test set (RMSE, MAE, MAPE, R²)
7. Save model + feature columns + country encoder

**Training Time:** 2-4 giờ trên CPU (tùy vào lượng dữ liệu)

**Output:**
- `models/lightgbm_global.pkl` hoặc `models/xgboost_global.pkl`
- File chứa: model, feature_cols, metrics, country_encoder

---

## Model Output

**Trained Models:**
- `models/lightgbm_global.pkl` - LightGBM model
- `models/xgboost_global.pkl` - XGBoost model

**Model File Structure:**
- `model`: Trained model object
- `feature_cols`: Danh sách tên các features model sử dụng
- `metrics`: Validation và test metrics (RMSE, MAE, MAPE, R²)
- `target_col`: "aqi_next"
- `country_encoder`: LabelEncoder để encode country codes

**Evaluation Metrics:**
- **RMSE**: Căn bậc hai của trung bình bình phương lỗi
- **MAE**: Trung bình giá trị tuyệt đối lỗi
- **MAPE**: Phần trăm lỗi trung bình (chỉ tính với giá trị > 0.1)
- **R²**: Hệ số xác định (độ phù hợp của model)

**Ví dụ:** Nếu RMSE = 20.46, nghĩa là sai số trung bình khoảng ±20.46 điểm AQI cho dự đoán 1 giờ sau.

---

## Real-time Inference

Khi sử dụng model để dự đoán AQI cho 1 giờ tiếp theo trong thời gian thực, hệ thống sẽ:

### 1. Thu thập Dữ liệu từ OpenAQ API

**Dữ liệu hiện tại (bắt buộc):**
- Gọi `GET /locations/{location_id}` để lấy thông tin trạm đo
- Gọi `GET /sensors/{sensor_id}/measurements/hourly` cho từng sensor để lấy measurements mới nhất
- Tính toán AQI hiện tại từ các measurements (convert đơn vị, tính AQI cho từng parameter, lấy max)

**Dữ liệu lịch sử (chỉ cho lag features - tối ưu tốc độ):**
- Gọi `GET /sensors/{sensor_id}/measurements/hourly` cho từng sensor với:
  - `datetime_from`: 1 giờ trước (±1h window) → lấy AQI 1h trước
  - `datetime_from`: 24 giờ trước (±1h window) → lấy AQI 24h trước
  - `datetime_from`: 168 giờ trước (±1h window) → lấy AQI 168h trước
- Tính AQI cho mỗi thời điểm (lấy max AQI từ tất cả sensors)
- **Không fetch rolling statistics** để tối ưu tốc độ inference

### 2. Feature Engineering (giống training)

**Time features:** Từ datetime hiện tại
- hour, day_of_week, month, day_of_year, is_weekend

**Lag features:** Từ historical data
- `aqi_lag_1h`: AQI 1 giờ trước (nếu có)
- `aqi_lag_24h`: AQI 24 giờ trước (nếu có)
- `aqi_lag_168h`: AQI 168 giờ trước (nếu có)
- Nếu không có đủ data, dùng current AQI

**Rolling statistics:** Không fetch từ API (tối ưu tốc độ)
- `aqi_mean_7d`: Dùng current AQI (giống training khi `min_periods=1` và chỉ có 1 data point)
- `aqi_std_7d`: 0 (không có độ lệch)
- `aqi_max_7d`: Dùng current AQI
- `aqi_min_7d`: Dùng current AQI
- `aqi_mean_30d`: Dùng current AQI
- **Lý do:** Để inference nhanh, không fetch 168-720 measurements từ API. Model vẫn hoạt động tốt vì đã được train với `min_periods=1` (có thể xử lý trường hợp chỉ có 1 data point)

**Spatial features:** Từ location info
- country_encoded (dùng encoder từ model)
- latitude, longitude

**Pollutant features:** Từ current measurements
- pm25, pm10, o3, co, so2, no2 (extract từ parameters/values, fill 0 nếu không có)

### 3. Prediction

- Đưa vector features vào model
- Model trả về `aqi_next` (AQI dự đoán cho 1 giờ tiếp theo)
- Tính category và recommendation từ predicted AQI

**Lưu ý:**
- Không sử dụng MinIO để tránh độ trễ
- Tất cả dữ liệu lấy từ OpenAQ API
- **Tối ưu tốc độ:** Chỉ fetch 3 lag features (1h, 24h, 168h trước), không fetch rolling statistics
- Rolling statistics dùng current AQI (giống training với `min_periods=1`)
- Cách tính features giống hệt training để đảm bảo độ chính xác

---

## File Structure

```
ml_training/
├── data_loader.py      # Load data + feature engineering
├── train_lightgbm.py   # Train LightGBM
├── train_xgboost.py    # Train XGBoost
├── evaluate.py         # Evaluate models
├── models/             # Saved models
│   ├── lightgbm_global.pkl
│   └── xgboost_global.pkl
└── results/            # Evaluation results, plots
```
