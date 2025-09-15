#!/bin/bash

# ========================================
#  E-Commerce ETL Pipeline Runner Script
#  Runs the PySpark ETL pipeline with JDBC
# ========================================

set -euo pipefail

# ----------------------------
# Utility: logging with time
# ----------------------------
log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# ----------------------------
# Resolve paths
# ----------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SRC_DIR="$PROJECT_ROOT/src"
JAR_PATH="$PROJECT_ROOT/libs/jars/mssql-jdbc-12.2.0.jre8.jar"

# ----------------------------
# Config (edit as needed)
# ----------------------------
SOURCE_DB="ecom_db"
TARGET_DB="ecom_dwh"
SOURCE_USER="sa"
SOURCE_PASSWORD="Gova#ss123"
TARGET_USER="sa"
TARGET_PASSWORD="Gova#ss123"
SERVER="localhost:1433"

SPARK_APP_NAME="ECommerce_ETL_Pipeline"
SPARK_MASTER="local[*]"

# ----------------------------
# JDBC URLs
# ----------------------------
SOURCE_JDBC_URL="jdbc:sqlserver://${SERVER};databaseName=${SOURCE_DB};encrypt=true;trustServerCertificate=true"
TARGET_JDBC_URL="jdbc:sqlserver://${SERVER};databaseName=${TARGET_DB};encrypt=true;trustServerCertificate=true"

# ----------------------------
# Pre-checks
# ----------------------------
if ! command -v spark-submit &>/dev/null; then
  log "❌ Error: spark-submit not found in PATH"
  exit 1
fi

if [ ! -f "$JAR_PATH" ]; then
  log "❌ Error: JDBC JAR not found at $JAR_PATH"
  log "👉 Please download mssql-jdbc-12.2.0.jre8.jar and place it in libs/jars"
  exit 1
fi

if [ ! -f "$SRC_DIR/ecom_etl_pipeline.py" ]; then
  log "❌ Error: $SRC_DIR/ecom_etl_pipeline.py not found"
  exit 1
fi

# ----------------------------
# Show config (mask passwords)
# ----------------------------
log "🚀 Starting E-Commerce ETL Pipeline..."
echo "----------------------------------------"
echo " Source DB   : $SOURCE_DB"
echo " Target DB   : $TARGET_DB"
echo " Server      : $SERVER"
echo " Spark Master: $SPARK_MASTER"
echo "----------------------------------------"
echo ""

# ----------------------------
# Run ETL
# ----------------------------
log "🔄 Running ETL Pipeline with spark-submit..."

spark-submit \
  --master "${SPARK_MASTER}" \
  --name "${SPARK_APP_NAME}" \
  --jars "${JAR_PATH}" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.sql.adaptive.skewJoin.enabled=true" \
  --conf "spark.sql.adaptive.localShuffleReader.enabled=true" \
  --conf "spark.sql.statistics.histogram.enabled=true" \
  --conf "spark.sql.cbo.enabled=true" \
  --conf "spark.sql.cbo.joinReorder.enabled=true" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.shuffle.partitions=200" \
  --conf "spark.sql.execution.arrow.pyspark.enabled=true" \
  "$SRC_DIR/ecom_etl_pipeline.py" \
  --source-jdbc-url "${SOURCE_JDBC_URL}" \
  --target-jdbc-url "${TARGET_JDBC_URL}" \
  --source-user "${SOURCE_USER}" \
  --source-password "${SOURCE_PASSWORD}" \
  --target-user "${TARGET_USER}" \
  --target-password "${TARGET_PASSWORD}" \
  --app-name "${SPARK_APP_NAME}" \
  --master "${SPARK_MASTER}"

EXIT_CODE=$?

# ----------------------------
# Post-run status
# ----------------------------
if [ $EXIT_CODE -eq 0 ]; then
  log "✅ ETL Pipeline completed successfully!"
  log "🎉 Data loaded into target DB: ${TARGET_DB}"
else
  log "❌ ETL Pipeline failed! Exit code: $EXIT_CODE"
  log "⚠️  Please check Spark logs for details."
  exit $EXIT_CODE
fi