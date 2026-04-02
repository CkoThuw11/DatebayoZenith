#!/bin/bash

# ==============================================================================
# Script deploy S3 Sink Connector lên Kafka Connect
# ==============================================================================

CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="minio-s3-sink-connector"
CONFIG_FILE="${1:-connectors/s3-sink-minio.json}"

echo "🚀 Deploy S3 Sink Connector"
echo "   Kafka Connect: $CONNECT_URL"
echo "   Config file:   $CONFIG_FILE"
echo ""

# 1. Kiểm tra Kafka Connect đã sẵn sàng chưa
echo "⏳ Đang kiểm tra Kafka Connect..."
MAX_RETRIES=30
RETRY=0
while [ $RETRY -lt $MAX_RETRIES ]; do
    STATUS=$(curl -s -o //dev//null -w "%{http_code}" "$CONNECT_URL/connectors" 2>//dev//null)
    if [ "$STATUS" = "200" ]; then
        echo "✅ Kafka Connect đã sẵn sàng!"
        break
    fi
    RETRY=$((RETRY + 1))
    echo "   Lần thử $RETRY/$MAX_RETRIES — Kafka Connect chưa sẵn sàng (HTTP $STATUS), đợi 5 giây..."
    sleep 5
done

if [ $RETRY -eq $MAX_RETRIES ]; then
    echo "❌ Kafka Connect không phản hồi sau $MAX_RETRIES lần thử. Thoát."
    exit 1
fi

# 2. Kiểm tra connector đã tồn tại chưa
EXISTING=$(curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME" 2>//dev//null | grep -c "\"name\"")
if [ "$EXISTING" -gt 0 ]; then
    echo "🔄 Connector '$CONNECTOR_NAME' đã tồn tại. Đang cập nhật..."
    # Lấy phần config từ JSON file
    CONFIG=$(python -c "import json,sys; d=json.load(open('$CONFIG_FILE')); print(json.dumps(d['config']))" 2>//dev//null || cat "$CONFIG_FILE" | python3 -c "import json,sys; d=json.load(sys.stdin); print(json.dumps(d['config']))")
    curl -s -X PUT "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" \
        -H "Content-Type: application/json" \
        -d "$CONFIG" | python -m json.tool 2>//dev//null || echo "(raw response above)"
else
    echo "📦 Đang deploy connector mới..."
    curl -s -X POST "$CONNECT_URL/connectors" \
        -H "Content-Type: application/json" \
        -d @"$CONFIG_FILE" | python -m json.tool 2>//dev//null || echo "(raw response above)"
fi

echo ""

# 3. Kiểm tra trạng thái connector
echo "📊 Trạng thái connector:"
sleep 3
curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | python -m json.tool 2>//dev//null || \
    curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status"

echo ""
echo "🎉 Hoàn tất! Kiểm tra MinIO Console tại http://localhost:9001"
