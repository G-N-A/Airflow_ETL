#!/bin/bash

# Configurable ETL Runner
# Usage: ./run_etl_config.sh [source_db] [target_db] [server]

# Default values
SOURCE_DB=${1:-"ecom_db"}
TARGET_DB=${2:-"ecom_dwh"}
SERVER=${3:-"localhost:1433"}

echo "🚀 Configurable ETL Pipeline"
echo "============================="
echo "Source DB: $SOURCE_DB"
echo "Target DB: $TARGET_DB"
echo "Server: $SERVER"
echo ""

# Prompt for credentials
read -p "Enter source username [sa]: " SOURCE_USER
SOURCE_USER=${SOURCE_USER:-"sa"}

read -s -p "Enter source password [Gova#ss123]: " SOURCE_PASSWORD
SOURCE_PASSWORD=${SOURCE_PASSWORD:-"Gova#ss123"}
echo ""

read -p "Enter target username [sa]: " TARGET_USER
TARGET_USER=${TARGET_USER:-"sa"}

read -s -p "Enter target password [Gova#ss123]: " TARGET_PASSWORD
TARGET_PASSWORD=${TARGET_PASSWORD:-"Gova#ss123"}
echo ""

# Build JDBC URLs
SOURCE_JDBC_URL="jdbc:sqlserver://${SERVER};databaseName=${SOURCE_DB};encrypt=true;trustServerCertificate=true"
TARGET_JDBC_URL="jdbc:sqlserver://${SERVER};databaseName=${TARGET_DB};encrypt=true;trustServerCertificate=true"

echo "🔄 Starting ETL Pipeline..."

spark-submit \
    --master "local[*]" \
    --name "ECommerce_ETL_${SOURCE_DB}_to_${TARGET_DB}" \
    --jars "/media/softsuave/DATA-HDD/DataEngineering/Apache_Airflow/Jars/mssql-jdbc-12.2.0.jre8.jar" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.execution.arrow.pyspark.enabled=true" \
    ecom_etl_pipeline.py \
    --source-jdbc-url "${SOURCE_JDBC_URL}" \
    --target-jdbc-url "${TARGET_JDBC_URL}" \
    --source-user "${SOURCE_USER}" \
    --source-password "${SOURCE_PASSWORD}" \
    --target-user "${TARGET_USER}" \
    --target-password "${TARGET_PASSWORD}"

if [ $? -eq 0 ]; then
    echo "✅ ETL Pipeline completed successfully!"
else
    echo "❌ ETL Pipeline failed!"
    exit 1
fi
