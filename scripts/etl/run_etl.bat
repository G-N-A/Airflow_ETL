@echo off
REM Windows Batch Script for ETL Pipeline
REM This script runs the PySpark ETL pipeline on Windows

echo 🚀 Starting E-Commerce ETL Pipeline...
echo ========================================

REM Configuration
set SOURCE_DB=ecom_db
set TARGET_DB=ecom_dwh
set SOURCE_USER=sa
set SOURCE_PASSWORD=Gova#ss123
set TARGET_USER=sa
set TARGET_PASSWORD=Gova#ss123
set SERVER=localhost:1433

REM JDBC URLs
set SOURCE_JDBC_URL=jdbc:sqlserver://%SERVER%;databaseName=%SOURCE_DB%;encrypt=true;trustServerCertificate=true
set TARGET_JDBC_URL=jdbc:sqlserver://%SERVER%;databaseName=%TARGET_DB%;encrypt=true;trustServerCertificate=true

REM Spark Configuration
set SPARK_APP_NAME=ECommerce_ETL_Pipeline
set JAR_PATH=Jars\mssql-jdbc-12.2.0.jre8.jar

echo 📊 Configuration:
echo   Source DB: %SOURCE_DB%
echo   Target DB: %TARGET_DB%
echo   Server: %SERVER%
echo.

REM Check if files exist
if not exist "%JAR_PATH%" (
    echo ❌ Error: JDBC JAR file not found at %JAR_PATH%
    echo Please download mssql-jdbc-12.2.0.jre8.jar and place it in the Jars directory
    pause
    exit /b 1
)

if not exist "ecom_etl_pipeline.py" (
    echo ❌ Error: ecom_etl_pipeline.py not found in current directory
    pause
    exit /b 1
)

echo 🔄 Running ETL Pipeline...

spark-submit ^
    --master "local[*]" ^
    --name "%SPARK_APP_NAME%" ^
    --jars "%JAR_PATH%" ^
    --conf "spark.sql.adaptive.enabled=true" ^
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" ^
    --conf "spark.sql.adaptive.skewJoin.enabled=true" ^
    --conf "spark.sql.adaptive.localShuffleReader.enabled=true" ^
    --conf "spark.sql.statistics.histogram.enabled=true" ^
    --conf "spark.sql.cbo.enabled=true" ^
    --conf "spark.sql.cbo.joinReorder.enabled=true" ^
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" ^
    --conf "spark.sql.shuffle.partitions=200" ^
    --conf "spark.sql.execution.arrow.pyspark.enabled=true" ^
    ecom_etl_pipeline.py ^
    --source-jdbc-url "%SOURCE_JDBC_URL%" ^
    --target-jdbc-url "%TARGET_JDBC_URL%" ^
    --source-user "%SOURCE_USER%" ^
    --source-password "%SOURCE_PASSWORD%" ^
    --target-user "%TARGET_USER%" ^
    --target-password "%TARGET_PASSWORD%" ^
    --app-name "%SPARK_APP_NAME%" ^
    --master "local[*]"

REM Check exit status
if %ERRORLEVEL% EQU 0 (
    echo.
    echo ✅ ETL Pipeline completed successfully!
    echo 🎉 Data has been processed and loaded into %TARGET_DB%
) else (
    echo.
    echo ❌ ETL Pipeline failed!
    echo Please check the error messages above
)

pause
