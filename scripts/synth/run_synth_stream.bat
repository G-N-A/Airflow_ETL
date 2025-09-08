@echo off
REM Synthetic E-commerce Data Stream Runner - Windows
REM This script runs the synthetic data generation and streaming to SQL Server

echo 🚀 Starting Synthetic E-commerce Data Stream...
echo ==============================================

REM Configuration
set DB_URL=mssql+pyodbc://sa:Gova%%23ss123@localhost:1433/ecom_db?driver=ODBC+Driver+17+for+SQL+Server^&TrustServerCertificate=yes
set WEBHOOK_URL=http://localhost:5000/webhook
set INTERVAL_MINUTES=15.0
set ORDERS_PER_INTERVAL=50
set CLICKS_PER_INTERVAL=100

echo 📊 Configuration:
echo   Database: ecom_db
echo   User: sa
echo   Streaming Interval: %INTERVAL_MINUTES% minutes
echo   Orders per interval: %ORDERS_PER_INTERVAL%
echo   Clicks per interval: %CLICKS_PER_INTERVAL%
echo   Webhook URL: %WEBHOOK_URL%
echo.

REM Check if Python script exists
if not exist "synth_ecom_mssql_stream.py" (
    echo ❌ Error: synth_ecom_mssql_stream.py not found in current directory
    pause
    exit /b 1
)

REM Check if virtual environment exists
if exist "airflow_venv\Scripts\activate.bat" (
    echo 🔧 Activating virtual environment...
    call airflow_venv\Scripts\activate.bat
)

REM Check database connection
echo 🔍 Checking database connection...
python -c "import pyodbc; conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=ecom_db;UID=sa;PWD=Gova#ss123;TrustServerCertificate=yes'); conn.close(); print('✅ Database connection successful')"

if %errorlevel% neq 0 (
    echo ❌ Cannot connect to database. Please check your SQL Server connection.
    pause
    exit /b 1
)

echo.
echo 🔄 Starting synthetic data streaming...
echo Press Ctrl+C to stop the stream
echo.

REM Run the synthetic data stream
python synth_ecom_mssql_stream.py ^
    --db-url "%DB_URL%" ^
    --webhook-url "%WEBHOOK_URL%" ^
    --interval-minutes "%INTERVAL_MINUTES%" ^
    --orders-per-interval "%ORDERS_PER_INTERVAL%" ^
    --clicks-per-interval "%CLICKS_PER_INTERVAL%"

REM Check exit status
if %errorlevel% equ 0 (
    echo.
    echo ✅ Synthetic data streaming completed successfully!
) else (
    echo.
    echo ❌ Synthetic data streaming failed!
    pause
    exit /b 1
)

pause
