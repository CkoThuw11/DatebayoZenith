#!/bin/bash
set -e

SLEEP_INTERVAL=${SLEEP_INTERVAL:-60}
SPARK_SUBMIT=${SPARK_SUBMIT_PATH:-/opt/spark/bin/spark-submit}

EXTRA_JARS="/opt/spark/extra-jars/spark-avro.jar,\
/opt/spark/extra-jars/hadoop-aws.jar,\
/opt/spark/extra-jars/aws-java-sdk-bundle.jar,\
/opt/spark/extra-jars/deequ.jar"

echo "===================================================="
echo " Spark CDC Processor - Scheduled Runner"
echo " Interval: ${SLEEP_INTERVAL}s"
echo " spark-submit: ${SPARK_SUBMIT}"
echo "===================================================="

# --------------------------------------------------
# Wait for Hive Metastore (port 9083)
# --------------------------------------------------
echo "[entrypoint] Waiting for Hive Metastore..."

until (echo > /dev/tcp/hive-metastore/9083) 2>/dev/null; do
  echo "[entrypoint] Hive Metastore not ready yet..."
  sleep 5
done

echo "[entrypoint] ✅ Hive Metastore is ready."

# --------------------------------------------------
# Wait for MinIO (HTTP health endpoint)
# --------------------------------------------------
echo "[entrypoint] Waiting for MinIO..."

until curl -sf http://minio:9000/minio/health/live > /dev/null; do
  echo "[entrypoint] MinIO not ready yet..."
  sleep 5
done

echo "[entrypoint] ✅ MinIO is ready."

# --------------------------------------------------
# Common spark-submit runner
# --------------------------------------------------
run_spark() {
  local script="$1"

  "${SPARK_SUBMIT}" \
    --master "local[2]" \
    --jars "/opt/spark/extra-jars/spark-avro.jar,/opt/spark/extra-jars/hadoop-aws.jar,/opt/spark/extra-jars/aws-java-sdk-bundle.jar,/opt/spark/extra-jars/deequ.jar"\
    --conf "spark.driver.memory=1g" \
    --conf "spark.executor.memory=1g" \
    "${script}"
}

# --------------------------------------------------
# STEP 1: Initialize Hive Metadata/Tables
# --------------------------------------------------
echo "[entrypoint] Initializing Hive Metastore structures..."

run_spark /app/spark-app/init_hive.py
INIT_EXIT_CODE=$?

if [ "${INIT_EXIT_CODE}" -ne 0 ]; then
  echo "[entrypoint] ❌ Hive initialization failed."
  exit 1
fi

echo "[entrypoint] ✅ Hive initialization completed."

# --------------------------------------------------
# STEP 2: CDC LOOP
# --------------------------------------------------
while true; do

  echo ""
  echo "[entrypoint] =================================="
  echo "[entrypoint] CDC RUN STARTED"
  echo "[entrypoint] $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "[entrypoint] =================================="

  run_spark /app/spark-app/cdc_processor.py
  EXIT_CODE=$?

  if [ "${EXIT_CODE}" -eq 0 ]; then
    echo "[entrypoint] ✅ Run completed successfully."
  else
    echo "[entrypoint] ⚠️ WARNING: Run exited with code ${EXIT_CODE}. Continuing..."
  fi

  echo "[entrypoint] Sleeping ${SLEEP_INTERVAL}s until next run..."
  sleep "${SLEEP_INTERVAL}"

done