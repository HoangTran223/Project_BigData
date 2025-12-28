# ML Training Pipeline - Air Quality Prediction

## Input

**Data Source**: MinIO Gold Layer (Parquet)
- **Path**: `s3a://air-quality-data/gold/year=YYYY/month=MM/day=DD/*.parquet`
- **Format**: Parquet (columnar, hourly aggregated AQI)
- **Time Range**: 2016-01-30 đến 2025-12-24 (data thực tế có sẵn)
- **Countries**: 9 quốc gia có data (ID, KH, LA, MM, MY, PH, SG, TH, VN)
- **Missing Countries**: BN (Brunei), TL (Timor-Leste)

**Số lượng trạm (stations) còn sống per country:**
| Country | Stations | Records | Date Range |
|---------|----------|---------|------------|
| TH (Thailand) | 320 | 4,444,405 | 2016-01-30 to 2025-12-24 |
| LA (Laos) | 131 | 646,596 | 2021-04-29 to 2025-12-24 |
| PH (Philippines) | 55 | 239,584 | 2023-09-06 to 2025-12-24 |
| KH (Cambodia) | 10 | 27,793 | 2025-04-10 to 2025-12-24 |
| MY (Malaysia) | 7 | 26,832 | 2024-12-23 to 2025-12-24 |
| SG (Singapore) | 7 | 42,643 | 2024-08-19 to 2025-12-24 |
| VN (Vietnam) | 6 | 36,417 | 2024-01-29 to 2025-12-24 |
| ID (Indonesia) | 5 | 22,632 | 2023-08-01 to 2025-12-24 |
| MM (Myanmar) | 1 | 1,971 | 2025-09-24 to 2025-12-23 |
| **Total** | **542** | **5,488,873** | - |
| BN (Brunei) | 0 | 0 | - (no alive stations) |
| TL (Timor-Leste) | 0 | 0 | - (no alive stations) |

**Lý do thiếu BN và TL:**
- Trong `collect_data.py`, chỉ thu thập từ các stations "alive" (có data trong 7 ngày qua)
- Chỉ lấy stations có ít nhất PM2.5 hoặc PM10 sensors
- BN và TL không có stations thỏa điều kiện này khi thu thập data
- **Kết quả**: BN và TL không có trong Kafka → không có trong Bronze/Silver/Gold layers
- **Giải pháp**: Nếu cần data BN/TL, có thể:
  1. Giảm `ALIVE_THRESHOLD_DAYS` trong `collect_data.py` (hiện tại = 7)
  2. Hoặc bỏ filter "alive stations" để lấy tất cả historical data
  3. Hoặc collect riêng cho BN/TL với điều kiện khác

**Note về số records:**
- Bronze layer: ~11M records (raw data, mỗi record = 1 parameter tại 1 thời điểm)
- Gold layer: ~5.4M records (aggregated by hour, mỗi record = 1 location/giờ với tất cả parameters)
- Số records giảm là bình thường do aggregation (nhiều parameters trong 1 giờ → 1 record)

**Data Columns**:
- `datetime`, `location_id`, `location_name`, `country`
- `latitude`, `longitude`
- `aqi`, `aqi_category`
- `parameters` (array), `values` (array)

## Output

**Trained Models**:
- `models/lightgbm_global.pkl` - LightGBM model
- `models/xgboost_global.pkl` - XGBoost model

**Model File Structure**:
```python
{
    'model': TrainedModel,
    'feature_cols': [...],
    'metrics': {'rmse': ..., 'mae': ..., 'mape': ..., 'r2': ...},
    'target_col': 'aqi_next',
    'train_date': '...',
    'country_encoder': LabelEncoder
}
```

**Evaluation Results**:
- Validation metrics (RMSE, MAE, MAPE, R²)
- Test metrics (RMSE, MAE, MAPE, R²)
- Feature importance plots
- Prediction visualizations

---

## Setup

### 1. Start Services

```bash
cd /home/user/Project
docker-compose up -d minio
```

**Kiểm tra**: Đợi 10-20 giây để MinIO khởi động xong, sau đó verify:
```bash
docker ps | grep minio
```

### 2. Install Dependencies

```bash
cd /home/user/Project
pip install -r requirements_ml.txt
```

### 3. Check Data (Optional)

```bash
cd /home/user/Project/ml_training
python check_countries.py
```

**Mục đích**: Kiểm tra countries và date ranges trong Gold layer
- **Expected**: 9 quốc gia (ID, KH, LA, MM, MY, PH, SG, TH, VN)
- **Missing**: BN, TL (xem giải thích ở phần Input)

---

## Phương pháp 1: LightGBM

### Quy trình Training

#### Bước 1: Load Data
```bash
cd /home/user/Project/ml_training
python train_lightgbm.py
```

**Mục đích**: Đọc dữ liệu từ MinIO Gold layer để training

**Thông số lấy**: 
- Parquet files từ `s3a://air-quality-data/gold/`
- Không filter date mặc định (lấy tất cả data để đảm bảo có đủ 11 quốc gia)
- Sort theo `location_id` và `datetime`

**Output**: DataFrame với ~5.4M+ records, 9 quốc gia (ID, KH, LA, MM, MY, PH, SG, TH, VN)
- **Note**: Thiếu BN, TL do không có "alive stations" khi collect (xem giải thích ở phần Input)

#### Bước 2: Feature Engineering
**Mục đích**: Tạo features từ raw data để model học được patterns

**Thông số lấy**:
- **Time**: hour, day_of_week, month, day_of_year, is_weekend (từ datetime)
- **Lag**: aqi_lag_1h, aqi_lag_24h, aqi_lag_168h (AQI từ 1h, 24h, 1 tuần trước)
- **Rolling**: aqi_mean_7d, aqi_std_7d, aqi_max_7d, aqi_min_7d, aqi_mean_30d
- **Spatial**: country_encoded, latitude, longitude
- **Pollutants**: pm25, pm10, o3, co, so2, no2 (extract từ arrays)
- **Target**: aqi_next (AQI giờ tiếp theo)

**Output**: DataFrame với ~30+ features + target `aqi_next`

#### Bước 3: Data Split
**Mục đích**: Chia data thành train/validation/test để training và đánh giá

**Thông số lấy**:
- Train: datetime <= 2023-12-31
- Validation: 2024-01-01 <= datetime <= 2024-12-31
- Test: datetime > 2024-12-31
- Remove rows có NaN trong target
- **Note**: Split dựa trên datetime thực tế của data, không cố định date range

**Output**: 3 DataFrames (train_df, val_df, test_df) với time-based split

#### Bước 4: Train Model
**Mục đích**: Train LightGBM model để học patterns từ features

**Thông số lấy**:
- Features: exclude metadata columns (datetime, location_id, country, ...)
- Hyperparameters: num_leaves=31, learning_rate=0.05, num_iterations=1000
- Early stopping: dừng nếu validation RMSE không improve sau 50 rounds

**Output**: Trained LightGBM model + validation metrics (RMSE, MAE, MAPE, R²)

#### Bước 5: Evaluate trên Test Set
**Mục đích**: Đánh giá model trên data chưa thấy (test set)

**Thông số lấy**: Test set features → predict → compare với actual AQI

**Output**: Test metrics (RMSE, MAE, MAPE, R²) printed to console

#### Bước 6: Save Model
**Mục đích**: Lưu model để dùng cho inference sau này

**Thông số lấy**: Model + feature_cols + metrics + country_encoder

**Output**: `models/lightgbm_global.pkl` file ready for inference

### Commands

```bash
cd /home/user/Project/ml_training
python train_lightgbm.py
```

---

## Phương pháp 2: XGBoost

### Quy trình Training

#### Bước 1: Load Data
```bash
cd /home/user/Project/ml_training
python train_xgboost.py
```

**Mục đích**: Đọc dữ liệu từ MinIO Gold layer để training

**Thông số lấy**: Giống LightGBM Bước 1 (không filter date mặc định)

**Output**: DataFrame với ~5.4M+ records, 9 quốc gia (ID, KH, LA, MM, MY, PH, SG, TH, VN)

#### Bước 2: Feature Engineering
**Mục đích**: Tạo features từ raw data để model học được patterns

**Thông số lấy**: Giống LightGBM Bước 2 (Time, Lag, Rolling, Spatial, Pollutants, Target)

**Output**: DataFrame với ~30+ features + target `aqi_next`

#### Bước 3: Data Split
**Mục đích**: Chia data thành train/validation/test để training và đánh giá

**Thông số lấy**: Giống LightGBM Bước 3 (Train <=2023, Val=2024, Test>2024)

**Output**: 3 DataFrames (train_df, val_df, test_df) với time-based split

#### Bước 4: Train Model
**Mục đích**: Train XGBoost model để học patterns từ features

**Thông số lấy**:
- Features: exclude metadata columns
- Hyperparameters: max_depth=6, learning_rate=0.05, n_estimators=1000
- Early stopping: dừng nếu validation RMSE không improve sau 50 rounds

**Output**: Trained XGBoost model + validation metrics (RMSE, MAE, MAPE, R²)

#### Bước 5: Evaluate trên Test Set
**Mục đích**: Đánh giá model trên data chưa thấy (test set)

**Thông số lấy**: Test set features → predict → compare với actual AQI

**Output**: Test metrics (RMSE, MAE, MAPE, R²) printed to console

#### Bước 6: Save Model
**Mục đích**: Lưu model để dùng cho inference sau này

**Thông số lấy**: Model + feature_cols + metrics + country_encoder

**Output**: `models/xgboost_global.pkl` file ready for inference

### Commands

```bash
cd /home/user/Project/ml_training
python train_xgboost.py
```

---

## So sánh 2 Phương pháp

| Aspect | LightGBM | XGBoost |
|--------|----------|---------|
| **Training Speed** | Nhanh hơn (2-3 hours) | Chậm hơn một chút (3-4 hours) |
| **Memory Usage** | Thấp hơn | Cao hơn |
| **Accuracy** | Tốt | Có thể tốt hơn một chút |
| **Hyperparameters** | num_leaves, feature_fraction | max_depth, subsample |
| **Best Use Case** | Large dataset, fast training | Slightly better accuracy needed |

**Recommendation**: Train cả 2, so sánh metrics, chọn model tốt hơn.

---

## Evaluation

### Evaluate Model

```bash
cd /home/user/Project/ml_training
python evaluate.py --model models/lightgbm_global.pkl
python evaluate.py --model models/xgboost_global.pkl
```

**Giải thích**:
- Load model từ file
- Load test data (2025)
- Create features
- Predict và calculate metrics
- Generate plots: predicted vs actual, residuals, feature importance
- **Output**: Metrics + plots trong `results/`

---

## File Structure

```
ml_training/
├── data_loader.py      # Load data + feature engineering (dùng chung)
├── train_lightgbm.py   # Train LightGBM
├── train_xgboost.py     # Train XGBoost
├── evaluate.py          # Evaluate models
├── models/             # Saved models
│   ├── lightgbm_global.pkl
│   └── xgboost_global.pkl
└── results/           # Evaluation results, plots
```
