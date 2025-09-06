#!/bin/bash

# ETL Pipeline Runner Script
# This script runs the PySpark ETL pipeline with predefined configurations

echo "🚀 Starting E-Commerce ETL Pipeline..."
echo "========================================"

# Configuration
SOURCE_DB="ecom_db"
TARGET_DB="ecom_dwh"
SOURCE_USER="sa"
SOURCE_PASSWORD="Gova#ss123"
TARGET_USER="sa"
TARGET_PASSWORD="Gova#ss123"
SERVER="localhost:1433"

# JDBC URLs
SOURCE_JDBC_URL="jdbc:sqlserver://${SERVER};databaseName=${SOURCE_DB};encrypt=true;trustServerCertificate=true"
TARGET_JDBC_URL="jdbc:sqlserver://${SERVER};databaseName=${TARGET_DB};encrypt=true;trustServerCertificate=true"

# Spark Configuration
SPARK_APP_NAME="ECommerce_ETL_Pipeline"
SPARK_MASTER="local[*]"
JAR_PATH="/media/softsuave/DATA-HDD/DataEngineering/Apache_Airflow/Jars/mssql-jdbc-12.2.0.jre8.jar"

# Check if JAR file exists
if [ ! -f "$JAR_PATH" ]; then
    echo "❌ Error: JDBC JAR file not found at $JAR_PATH"
    echo "Please download mssql-jdbc-12.2.0.jre8.jar and place it in the Jars directory"
    exit 1
fi

# Check if Python script exists
if [ ! -f "ecom_etl_pipeline.py" ]; then
    echo "❌ Error: ecom_etl_pipeline.py not found in current directory"
    exit 1
fi

echo "📊 Configuration:"
echo "  Source DB: ${SOURCE_DB}"
echo "  Target DB: ${TARGET_DB}"
echo "  Server: ${SERVER}"
echo "  Spark Master: ${SPARK_MASTER}"
echo ""

# Run the ETL pipeline
echo "🔄 Running ETL Pipeline..."
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
    ecom_etl_pipeline.py \
    --source-jdbc-url "${SOURCE_JDBC_URL}" \
    --target-jdbc-url "${TARGET_JDBC_URL}" \
    --source-user "${SOURCE_USER}" \
    --source-password "${SOURCE_PASSWORD}" \
    --target-user "${TARGET_USER}" \
    --target-password "${TARGET_PASSWORD}" \
    --app-name "${SPARK_APP_NAME}" \
    --master "${SPARK_MASTER}"

# Check exit status
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ ETL Pipeline completed successfully!"
    echo "🎉 Data has been processed and loaded into ${TARGET_DB}"
else
    echo ""
    echo "❌ ETL Pipeline failed!"
    echo "Please check the error messages above"
    exit 1
fi
