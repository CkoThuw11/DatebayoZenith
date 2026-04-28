#!/bin/bash
# entrypoint.sh - Scheduler loop for the CDC Processor
# Runs cdc_processor.py on every SLEEP_INTERVAL seconds (default: 180s = 3 min)

SLEEP_INTERVAL=${SLEEP_INTERVAL:-180}

# spark-submit location in the official apache/spark image
SPARK_SUBMIT=${SPARK_SUBMIT_PATH:-/opt/spark/bin/spark-submit}

echo "=============================================="
echo "  Spark CDC Processor - Scheduled Runner"
echo "  Interval: ${SLEEP_INTERVAL}s"
echo "  spark-submit: ${SPARK_SUBMIT}"
echo "=============================================="

# Wait for MinIO to be reachable before first run
echo "[entrypoint] Waiting 30s for upstream services to stabilise..."
sleep 30

while true; do
  echo ""
  echo "[entrypoint] $(date -u '+%Y-%m-%dT%H:%M:%SZ') - Starting CDC processing run..."

  "${SPARK_SUBMIT}" \
    --master "local[2]" \
    --jars "/opt/spark/extra-jars/spark-avro.jar,/opt/spark/extra-jars/hadoop-aws.jar,/opt/spark/extra-jars/aws-java-sdk-bundle.jar,/opt/spark/extra-jars/deequ.jar" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.driver.memory=1g" \
    --conf "spark.executor.memory=1g" \
    /app/cdc_processor.py

  EXIT_CODE=$?

  if [ "${EXIT_CODE}" -eq 0 ]; then
    echo "[entrypoint] Run completed successfully."
  else
    echo "[entrypoint] WARNING: Run exited with code ${EXIT_CODE}. Continuing to next cycle."
  fi

  echo "[entrypoint] Sleeping ${SLEEP_INTERVAL}s until next run..."
  sleep "${SLEEP_INTERVAL}"
done
