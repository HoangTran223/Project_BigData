#!/bin/bash
# Script để xóa toàn bộ dữ liệu đã lưu trong hệ thống
# CẢNH BÁO: Script này sẽ xóa TẤT CẢ dữ liệu, không thể khôi phục!

set -e

echo "=========================================="
echo "⚠️  CẢNH BÁO: Script này sẽ xóa TẤT CẢ dữ liệu!"
echo "=========================================="
read -p "Bạn có chắc chắn muốn tiếp tục? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Đã hủy."
    exit 0
fi

echo ""
echo "Bắt đầu cleanup..."

# 1. Dừng các services (quan trọng: phải dừng Spark trước để xóa checkpoint)
echo "[1/7] Dừng các services..."
cd /home/user/Project
docker-compose stop spark-streaming clickhouse 2>/dev/null || true
sleep 2

# 2. Xóa dữ liệu trong MinIO (Bronze, Silver, Gold, Checkpoints)
echo "[2/7] Xóa dữ liệu trong MinIO..."
python3 << 'PYTHON_SCRIPT'
import s3fs
import sys

try:
    fs = s3fs.S3FileSystem(
        key='minioadmin',
        secret='minioadmin123',
        endpoint_url='http://localhost:9000',
        use_ssl=False
    )
    
    # Xóa các thư mục
    paths_to_delete = [
        'air-quality-data/bronze',
        'air-quality-data/silver',
        'air-quality-data/gold',
        'air-quality-data/checkpoints'
    ]
    
    for path in paths_to_delete:
        try:
            if fs.exists(path):
                print(f"  Xóa {path}...")
                fs.rm(path, recursive=True)
                print(f"  ✓ Đã xóa {path}")
            else:
                print(f"  - {path} không tồn tại")
        except Exception as e:
            print(f"  ✗ Lỗi khi xóa {path}: {e}")
    
    print("✓ Đã xóa xong dữ liệu trong MinIO")
except Exception as e:
    print(f"✗ Lỗi kết nối MinIO: {e}")
    sys.exit(1)
PYTHON_SCRIPT

# 3. Xóa dữ liệu trong ClickHouse
echo "[3/7] Xóa dữ liệu trong ClickHouse..."
docker-compose exec -T clickhouse clickhouse-client --password default123 << 'CLICKHOUSE_SQL' 2>/dev/null || echo "  ClickHouse không chạy, bỏ qua"
TRUNCATE TABLE IF EXISTS bronze_measurements;
TRUNCATE TABLE IF EXISTS silver_measurements;
TRUNCATE TABLE IF EXISTS gold_hourly_aqi;
CLICKHOUSE_SQL

# 4. Xóa Kafka topic và messages
echo "[4/7] Xóa Kafka topic và messages..."
docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic openaq-raw-measurements 2>/dev/null || echo "  Topic không tồn tại hoặc đã bị xóa"
sleep 2
docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --create --topic openaq-raw-measurements --partitions 1 --replication-factor 1 2>/dev/null || echo "  Topic đã tồn tại"

# 5. Xóa Docker logs
echo "[5/7] Xóa Docker logs..."
docker-compose logs --tail=0 spark-streaming > /dev/null 2>&1 || true
docker-compose logs --tail=0 clickhouse > /dev/null 2>&1 || true
docker-compose logs --tail=0 kafka > /dev/null 2>&1 || true
docker-compose logs --tail=0 minio > /dev/null 2>&1 || true
echo "  ✓ Đã clear logs"

# 6. Xóa Spark checkpoints (quan trọng để Spark đọc lại từ đầu)
echo "[6/7] Xóa Spark checkpoints..."
# CHỈ xóa checkpoints, KHÔNG xóa blockmgr (Spark cần blockmgr để shuffle)
# Checkpoints đã được xóa ở bước 2 (MinIO)
# KHÔNG xóa /tmp/spark-* vì có thể chứa blockmgr đang được Spark sử dụng
echo "  ✓ Đã xóa Spark checkpoints (trong MinIO)"
echo "  → Spark sẽ đọc từ đầu Kafka topic khi restart (startingOffsets: earliest)"
echo "  → Lưu ý: KHÔNG xóa blockmgr folders trong /tmp (Spark cần để shuffle)"

# 7. Prune Docker system (optional - giải phóng thêm dung lượng)
echo "[7/7] Cleanup Docker system..."
docker system prune -f --volumes 2>/dev/null || true
echo "  ✓ Đã cleanup Docker"

echo ""
echo "=========================================="
echo "✅ Cleanup hoàn tất!"
echo "=========================================="
echo ""
echo "Dữ liệu đã được xóa:"
echo "  - MinIO: Bronze, Silver, Gold, Checkpoints"
echo "  - ClickHouse: Tất cả tables"
echo "  - Kafka: Topic và messages"
echo "  - Docker logs"
echo ""
echo "Bạn có thể chạy lại từ đầu theo openaq_kich_ban.md"

