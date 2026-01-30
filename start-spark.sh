#!/bin/bash

# Start Spark master
$SPARK_HOME/sbin/start-master.sh 

sleep 2

MASTER_URL="spark://$(hostname):7077"

# Start Spark worker connected to master
$SPARK_HOME/sbin/start-worker.sh $MASTER_URL

echo "Spark Master URL: $MASTER_URL"

# Keep container alive
tail -f /dev/null