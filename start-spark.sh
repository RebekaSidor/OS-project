#!/bin/bash

# Ξεκινάει ο Master
$SPARK_HOME/sbin/start-master.sh 

sleep 2

# Δυναμική εύρεση του Hostname
MASTER_URL="spark://$(hostname):7077"

# Ξεκινάει ο Worker
$SPARK_HOME/sbin/start-worker.sh $MASTER_URL

echo "Spark Master URL: $MASTER_URL"

# ΤΡΕΞΕ ΤΑ SCRIPTS ΑΥΤΟΜΑΤΑ
echo "Starting Parquet Conversion..."
spark-submit --master $MASTER_URL /app/convert_csv_to_parquet.py

echo "Starting Analysis..."
spark-submit --master $MASTER_URL /app/analysis_spark.py

echo "All tasks completed! Results are in /output."

# Κρατάμε το container ζωντανό για το Web UI
tail -f /opt/spark/logs/*