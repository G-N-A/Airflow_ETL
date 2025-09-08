## Ecommerce Data Platform – Project Overview and Operations (Current)

This document summarizes the current project structure, data workflows, schedules, and operations for the Ecommerce Data Platform implemented on Apache Airflow, PySpark, and SQL Server.

### 1) Folder Structure

- **src**: All Python source code
  - `src/synth_ecom_mssql_stream.py`: Synthetic e-commerce data generator and streamer to SQL Server
  - `src/ecom_etl_pipeline.py`: PySpark ETL pipeline (extract → clean/transform → load)
- **scripts**: Executable scripts (Linux/macOS, .sh only)
  - `scripts/synth/run_synth_stream.sh`: Runs the synthetic stream script
  - `scripts/etl/run_etl.sh`: Runs the ETL pipeline with Spark + JDBC driver
  - `scripts/airflow/run_airflow.sh`: Starts/stops/configures Airflow (webserver + scheduler)
  - Note: Windows `.bat` files are not used in this project and have been removed.
- **dags**: Airflow DAG definitions
  - `dags/ecom_schedules.py`: DAGs that schedule the synth stream and ETL pipeline
- **libs/jars**: JDBC drivers and external jars
  - `libs/jars/mssql-jdbc-12.2.0.jre8.jar`: SQL Server JDBC driver
- **logs**: Central logs directory (Airflow and custom)
- **data**: Optional data exports or local staging (if needed)
- **docs**: Project documentation (optional)

### 2) Technology Stack

- **Orchestration**: Apache Airflow
- **Processing**: PySpark (Spark SQL and MLlib)
- **Database**: Microsoft SQL Server (OLTP source: `ecom_db`; DWH target: `ecom_dwh`)
- **Drivers**: Microsoft JDBC for SQL Server
- **Scripting**: Python 3.x, Bash

### 3) Workflows

#### A. Synthetic Data Stream (OLTP)

- Script: `src/synth_ecom_mssql_stream.py`
- Purpose: Generate customers, products, orders, and clickstream events and insert into SQL Server source DB (`ecom_db`).
- Features:
  - Optional `--db-url` with default; multiple connection fallback options
  - Hardcoded defaults for data volumes and 15-min streaming interval inside the script
  - Webhook notifications on insert batches (configurable URL)

Run standalone:
```bash
bash /media/softsuave/DATA-HDD/DataEngineering/Apache_Airflow/scripts/synth/run_synth_stream.sh
```

#### B. ETL Pipeline (to DWH)

- Script: `src/ecom_etl_pipeline.py`
- Purpose: Extract from `ecom_db`, clean and transform, then load curated tables into `ecom_dwh` via JDBC.
- Key handling:
  - Robust casting for Spark arrays to strings before JDBC write
  - Numeric difference calculations via explicit casting for Spark Columns

Run standalone:
```bash
bash /media/softsuave/DATA-HDD/DataEngineering/Apache_Airflow/scripts/etl/run_etl.sh
```

### 4) Airflow DAGs and Schedules

File: `dags/ecom_schedules.py`

- **DAG: `synth_stream_5min`**
  - Description: Generate synthetic e-commerce data every 5 minutes (start: 2025-09-08 12:35 Asia/Kolkata)
  - Operator: `BashOperator` → `scripts/synth/run_synth_stream.sh`

- **DAG: `ecom_etl_10min`**
  - Description: Run ETL pipeline every 10 minutes (start: 2025-09-08 12:40 Asia/Kolkata)
  - Operator: `BashOperator` → `scripts/etl/run_etl.sh`

Note: If you want different schedules (e.g., every 1 minute and every 2 minutes), update `schedule_interval` in `dags/ecom_schedules.py` accordingly and reload the scheduler. The current code schedules are 5 minutes (synth) and 10 minutes (ETL).

### 5) Airflow Operations

Start/Restart Airflow services (webserver on port 8085 and scheduler):
```bash
bash /media/softsuave/DATA-HDD/DataEngineering/Apache_Airflow/scripts/airflow/run_airflow.sh
```

List DAGs:
```bash
airflow dags list | cat
```

Enable and trigger DAGs:
```bash
airflow dags unpause synth_stream_5min
airflow dags unpause ecom_etl_10min

airflow dags trigger synth_stream_5min
airflow dags trigger ecom_etl_10min
```

View recent task run logs under: `$AIRFLOW_HOME/logs/`

### 6) Database Connectivity

- Default SQL Server authentication for `sa` is configured in the scripts.
- Ensure the following databases exist and are accessible:
  - Source DB: `ecom_db`
  - Target DWH: `ecom_dwh`

### 7) Data Quality and Transformations (Highlights)

- Orders and order_items consistency checks (e.g., totals casting and comparisons)
- Customer 360 aggregations with arrays converted to CSV strings for JDBC compatibility
- Clickstream/session features prepared for analytics

### 8) Log Management

- Airflow writes logs under `$AIRFLOW_HOME/logs` organized by `dag_id`, `run_id`, `task_id`.
- To control log growth, you can periodically prune older `run_id=*` folders per DAG, keeping the latest N. A simple manual approach:
```bash
export AIRFLOW_HOME=/media/softsuave/DATA-HDD/DataEngineering/Apache_Airflow
find "$AIRFLOW_HOME/logs" -maxdepth 1 -type d -name 'dag_id=*' | while read -r dag; do
  ls -1t "$dag"/run_id=* 2>/dev/null | tail -n +6 | xargs -r rm -rf --
done
```

If you prefer automated pruning via Airflow, you can add a dedicated cleanup DAG to keep only the latest 5 runs per DAG. This is not currently included in code.

### 9) Operational Notes

- Ensure virtual environment activation paths in scripts are correct: `airflow_venv/bin/activate`.
- The JDBC driver path in `scripts/etl/run_etl.sh` is set to `libs/jars/mssql-jdbc-12.2.0.jre8.jar`.
- If DAGs do not appear, verify `AIRFLOW_HOME` and `AIRFLOW__CORE__DAGS_FOLDER` environment variables are set before starting services.

### 10) Next Steps (Optional)

- Add an hourly Airflow cleanup DAG to prune logs (keep last 5 runs).
- Parameterize data volumes and webhook URL via Airflow Variables.
- Add data validation tasks and alerts (email/webhook) in DAGs.


