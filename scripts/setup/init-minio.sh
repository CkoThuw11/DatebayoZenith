#!/bin/bash

# ==============================================================================
# Script khởi tạo MinIO: Tạo bucket và phân quyền
# ==============================================================================

# Tên bucket theo đúng "Hợp đồng Kỹ thuật" của nhóm
BUCKET_NAME="northwind-data-lake"

# Lấy thông tin đăng nhập từ file .env (nếu có), nếu không có sẽ dùng giá trị mặc định
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

MINIO_USER="${MINIO_ROOT_USER:-admin}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-admin123}"

# Cổng 9000 là cổng API của MinIO (không phải cổng giao diện 9001)
MINIO_URL="http://minio:9000"

echo "🚀 Bắt đầu cấu hình MinIO..."

# Đợi vài giây để đảm bảo container MinIO trong docker-compose đã thực sự "sống"
echo "⏳ Đang đợi MinIO server khởi động..."
sleep 5

# Chạy tất cả lệnh mc trong MỘT container duy nhất để alias được giữ lại
echo "🔗 Đang kết nối tới MinIO và cấu hình bucket..."
docker run --rm --network datebayozenith_default --entrypoint //bin/sh minio/mc -c "
  mc alias set myminio $MINIO_URL $MINIO_USER $MINIO_PASS &&
  echo '🪣 Đang tạo bucket: $BUCKET_NAME...' &&
  mc mb myminio/$BUCKET_NAME --ignore-existing &&
  echo '🔓 Đang thiết lập quyền truy cập cho bucket...' &&
  mc anonymous set public myminio/$BUCKET_NAME
"

echo "✅ Hoàn tất! Bucket '$BUCKET_NAME' đã sẵn sàng để nhận dữ liệu từ Kafka."
