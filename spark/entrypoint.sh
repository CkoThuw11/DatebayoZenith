#!/bin/bash

SLEEP_INTERVAL=${SLEEP_INTERVAL:-180}
SPARK_SUBMIT=/opt/spark/bin/spark-submit

echo "=============================================="
echo "  Spark CDC Processor"
echo "  Interval: ${SLEEP_INTERVAL}s"
echo "=============================================="

echo "[entrypoint] Waiting 30s..."
sleep 30

# -------------------------
# INIT HIVE
# -------------------------
echo "[entrypoint] Initializing Hive..."

${SPARK_SUBMIT} \
  --master local[2] \
  --jars /opt/spark/extra-jars/hadoop-aws.jar,/opt/spark/extra-jars/aws-java-sdk-bundle.jar,/opt/spark/extra-jars/spark-avro.jar \
  /app/init_hive.py

if [ $? -ne 0 ]; then
  echo "[entrypoint] Hive init failed"
  exit 1
fi

echo "[entrypoint] Hive init done"

# -------------------------
# LOOP
# -------------------------
while true; do
  echo ""
  echo "[entrypoint] $(date -u '+%Y-%m-%dT%H:%M:%SZ') Run CDC..."

  timeout 300 ${SPARK_SUBMIT} \
    --master local[2] \
    --jars /opt/spark/extra-jars/hadoop-aws.jar,/opt/spark/extra-jars/aws-java-sdk-bundle.jar,/opt/spark/extra-jars/spark-avro.jar \
    /app/cdc_processor.py

  EXIT_CODE=$?

  if [ "${EXIT_CODE}" -eq 0 ]; then
    echo "[entrypoint] Success"
  elif [ "${EXIT_CODE}" -eq 124 ]; then
    echo "[entrypoint] Timeout"
  else
    echo "[entrypoint] Failed code=${EXIT_CODE}"
  fi

  sleep ${SLEEP_INTERVAL}
done