# Quick Start Guide

## 1. Cài đặt Dependencies

```bash
pip install -r requirements.txt
```

## 2. Khởi động Docker Services

```bash
docker-compose up -d
```

Đợi khoảng 30-60 giây để tất cả services khởi động.

## 3. Kiểm tra Services

```bash
docker-compose ps
```

## 4. Setup MinIO

```bash
python setup_minio_buckets.py
```

## 5. Thu thập Dữ liệu

```bash
python collect_data.py --mode historical --days 7 --sample 5
```

## 6. Khởi động Spark Streaming

```bash
docker-compose up -d --build spark-streaming
docker-compose logs -f spark-streaming
```

## 7. Load vào ClickHouse

Sau khi Spark đã xử lý dữ liệu (đợi vài phút):

```bash
python load_to_clickhouse.py --layer all
```

## 8. Truy vấn ClickHouse

```bash
docker exec -it clickhouse clickhouse-client

SELECT count(*) FROM gold_hourly_aqi;
SELECT * FROM gold_hourly_aqi ORDER BY datetime DESC LIMIT 10;
```

## Dừng Services

```bash
docker-compose down
```
