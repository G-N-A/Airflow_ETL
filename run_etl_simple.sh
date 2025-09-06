#!/bin/bash

# Simple ETL Runner - Quick Start
echo "🚀 Quick ETL Pipeline Start"
echo "============================"

# Run with minimal configuration
spark-submit \
    --master "local[*]" \
    --jars "/media/softsuave/DATA-HDD/DataEngineering/Apache_Airflow/Jars/mssql-jdbc-12.2.0.jre8.jar" \
    ecom_etl_pipeline.py \
    --source-jdbc-url "jdbc:sqlserver://localhost:1433;databaseName=ecom_db;encrypt=true;trustServerCertificate=true" \
    --target-jdbc-url "jdbc:sqlserver://localhost:1433;databaseName=ecom_dwh;encrypt=true;trustServerCertificate=true" \
    --source-user "sa" \
    --source-password "Gova#ss123" \
    --target-user "sa" \
    --target-password "Gova#ss123"

echo "✅ ETL Complete!"
