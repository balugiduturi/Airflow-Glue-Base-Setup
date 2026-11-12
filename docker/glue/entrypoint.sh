#!/usr/bin/env bash
# ==============================================================================
# AWS Glue 4.0 PySpark Container Entrypoint
# Starts Spark History Server, Livy REST Server, and JupyterLab IDE
# ==============================================================================

set -e

echo "============================================================"
echo "[GLUE-INIT] Initializing AWS Glue 4.0 Runtime Environment..."
echo "============================================================"

# 1. Ensure Spark event log directory exists for Spark History Server
mkdir -p /tmp/spark-events

# 2. Configure Spark logging to WARN to prevent log spam in terminals
if [ -f "/home/glue_user/spark/conf/log4j2.properties" ]; then
    sed -i 's/rootLogger.level = info/rootLogger.level = warn/g' /home/glue_user/spark/conf/log4j2.properties 2>/dev/null || true
fi

# 3. Start Spark History Server (Port 18080) in background if available
if [ -f "/home/glue_user/spark/sbin/start-history-server.sh" ]; then
    echo "[GLUE-INIT] Starting Spark History Server on port 18080..."
    /home/glue_user/spark/sbin/start-history-server.sh || echo "[WARN] Failed to start Spark History Server"
fi

# 4. Start Apache Livy REST Server (Port 8998) if available
if command -v livy-server &> /dev/null; then
    echo "[GLUE-INIT] Starting Livy REST Server on port 8998..."
    livy-server start || echo "[WARN] Failed to start Livy Server"
fi

echo "============================================================"
echo "[GLUE-INIT] Starting JupyterLab on port 8888..."
echo "============================================================"

# 5. Launch JupyterLab with passwordless local access
exec jupyter lab \
    --no-browser \
    --ip=0.0.0.0 \
    --port=8888 \
    --allow-root \
    --ServerApp.root_dir=/home/glue_user/workspace/ \
    --ServerApp.token='' \
    --ServerApp.password=''

