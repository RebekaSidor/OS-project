#!/bin/bash

set -e  # exit on any error

# -------------------------------
# Start Spark Master
# -------------------------------
echo "Starting Spark Master..."
$SPARK_HOME/sbin/start-master.sh

# Wait until Master UI is ready
echo "Waiting for Spark Master UI..."
until curl -s http://localhost:8080 >/dev/null; do
    sleep 1
done

MASTER_URL="spark://$(hostname):7077"
echo "Spark Master is ready at $MASTER_URL"

# -------------------------------
# Start Spark Worker
# -------------------------------
echo "Starting Spark Worker..."
$SPARK_HOME/sbin/start-worker.sh $MASTER_URL

# Wait a few seconds to ensure worker registration
sleep 3

# -------------------------------
# Start Spark History Server
# -------------------------------
echo "Starting Spark History Server..."
mkdir -p /opt/spark/logs
$SPARK_HOME/sbin/start-history-server.sh

# -------------------------------
# Submit jobs
# -------------------------------
echo "Starting Parquet Conversion..."
spark-submit \
    --master $MASTER_URL \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=file:///opt/spark/logs \
    /app/convert_csv_to_parquet.py

echo "Starting Analysis..."
spark-submit \
    --master $MASTER_URL \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=file:///opt/spark/logs \
    /app/analysis_spark.py

echo "All tasks completed! Results are in /output."

# -------------------------------
# Keep container alive for monitoring
# -------------------------------
echo "Container is running. Spark UI: http://localhost:8080 | History Server: http://localhost:18080"
tail -f /opt/spark/logs/*
