```markdown
# Ecommerce Data Platform – Detailed Project Documentation

This documentation provides a comprehensive overview of the **Ecommerce Data Platform** built using **Apache Airflow**, **PySpark**, and **SQL Server**.  
It covers the **project structure, technologies, workflows, Airflow scheduling, operations, and best practices**, ensuring that both developers and operators can understand, maintain, and extend the system effectively.

---

## 1. Project Structure

The platform follows a modular structure where **source code, scripts, configurations, and DAGs** are organized for clarity and maintainability:

```

/project-root
│
├── src/                        # Python application logic
│   ├── synth\_ecom\_mssql\_stream.py   # Generates and streams synthetic e-commerce data
│   └── ecom\_etl\_pipeline.py         # PySpark ETL pipeline for DWH load
│
├── scripts/                    # Execution layer (shell scripts)
│   ├── synth/run\_synth\_stream.sh    # Wrapper to run synthetic stream generator
│   ├── etl/run\_etl.sh               # Wrapper to execute ETL pipeline
│   └── airflow/run\_airflow\.sh       # Starts/stops Airflow services
│
├── dags/                       # Orchestration layer
│   └── ecom\_schedules.py            # DAG definitions for stream + ETL
│
├── libs/jars/                  # External dependencies
│   └── mssql-jdbc-12.2.0.jre8.jar   # SQL Server JDBC driver
│
├── logs/                       # Centralized log repository
├── data/                       # Optional raw exports or staging files
└── docs/                       # Documentation

````

**Why this structure?**  
- `src`: Keeps all Python logic isolated.  
- `scripts`: Provides platform-agnostic entry points (bash only, no Windows `.bat`).  
- `dags`: Clear separation of orchestration logic from ETL code.  
- `libs/jars`: Makes JDBC drivers reusable across Spark jobs.  
- `logs`: Centralized for Airflow and application logs, simplifying monitoring.  

---

## 2. Technology Stack

The platform integrates multiple tools for **data generation, processing, orchestration, and storage**:

- **Apache Airflow** → Workflow orchestration, scheduling synthetic streams & ETL jobs.  
- **PySpark** → Distributed ETL processing, schema management, transformations.  
- **Microsoft SQL Server** → Primary storage:  
  - `ecom_db`: OLTP source (raw data).  
  - `ecom_dwh`: Data warehouse (transformed).  
- **Python 3.x** → Core programming language for both streaming and ETL scripts.  
- **JDBC Driver** → Connects PySpark to SQL Server for reliable reads/writes.  
- **Bash Scripts** → Standardized job entrypoints, ensuring repeatable execution.  

---

## 3. Workflows

### A. Synthetic Data Stream (OLTP Layer)

**Purpose:**  
Simulates an **operational database** with customers, orders, products, and clickstream data.  
This allows downstream ETL to always have a fresh dataset for testing analytics.

**Details:**  
- Script: `src/synth_ecom_mssql_stream.py`  
- Inserts synthetic batches into `ecom_db` every 15 minutes.  
- Batch size: ~50 orders and 100 click events per interval.  
- Optional webhook integration for monitoring/alerts.  

**Execution:**
```bash
bash scripts/synth/run_synth_stream.sh
````

---

### B. ETL Pipeline (DWH Layer)

**Purpose:**
Extracts from the OLTP database (`ecom_db`), applies **cleaning, casting, and transformations**, then loads into the data warehouse (`ecom_dwh`).

**Details:**

* Script: `src/ecom_etl_pipeline.py`
* Handles schema evolution gracefully.
* Converts Spark arrays to strings before writing (JDBC limitation).
* Performs numeric consistency checks (order totals, discounts, etc.).

**Execution:**

```bash
bash scripts/etl/run_etl.sh
```

---

## 4. Airflow DAGs & Scheduling

File: `dags/ecom_schedules.py`

The platform uses Airflow to orchestrate **both streaming and ETL jobs**:

* **DAG: `synth_stream_5min`**

  * Runs synthetic data stream every **5 minutes**.
  * Starts at `2025-09-08 12:35 IST`.
  * Operator: `BashOperator` → `scripts/synth/run_synth_stream.sh`.

* **DAG: `ecom_etl_10min`**

  * Runs ETL pipeline every **10 minutes**.
  * Starts at `2025-09-08 12:40 IST`.
  * Operator: `BashOperator` → `scripts/etl/run_etl.sh`.

**Why Airflow?**
Airflow provides visibility (UI + logs), scheduling, retries, and dependency management.
This ensures the **stream → ETL → analytics** workflow is **repeatable and fault-tolerant**.

---

## 5. Airflow Operations

### Start/Restart Airflow

```bash
bash scripts/airflow/run_airflow.sh
```

### List DAGs

```bash
airflow dags list
```

### Enable & Trigger DAGs

```bash
airflow dags unpause synth_stream_5min
airflow dags unpause ecom_etl_10min

airflow dags trigger synth_stream_5min
airflow dags trigger ecom_etl_10min
```

### Logs Location

```
$AIRFLOW_HOME/logs/
```

Airflow organizes logs by:

```
logs/dag_id=<dag_name>/run_id=<run_id>/task_id=<task_name>/
```

---

## 6. Database Setup

* Ensure SQL Server has the following databases:

  * `ecom_db` → Synthetic OLTP data
  * `ecom_dwh` → Transformed DWH tables

* Connection details (configured in scripts):

  * User: `sa`
  * Password: `<configured>`
  * Driver: `ODBC Driver 17 for SQL Server`

---

## 7. Data Quality & Transformations

Key quality checks performed during ETL:

1. **Order & Item Consistency**

   * Order totals match sum of items after discounts.

2. **Customer 360 Features**

   * Arrays (e.g., last N purchases) are flattened into CSV strings for compatibility.

3. **Clickstream Data**

   * Sessionization & feature engineering for downstream analytics.

---

## 8. Log Management

### Problem:

Airflow logs grow quickly due to frequent schedules.

### Manual Pruning:

```bash
export AIRFLOW_HOME=/media/softsuave/DATA-HDD/DataEngineering/Apache_Airflow
find "$AIRFLOW_HOME/logs" -maxdepth 1 -type d -name 'dag_id=*' | while read -r dag; do
  ls -1t "$dag"/run_id=* 2>/dev/null | tail -n +6 | xargs -r rm -rf --
done
```

### Best Practice:

Create a **log cleanup DAG** that runs hourly and keeps only the last 5 runs.

---

## 9. Operational Notes

* Always activate the correct virtual environment:

  ```
  source airflow_venv/bin/activate
  ```
* Ensure the JDBC driver is correctly referenced in `scripts/etl/run_etl.sh`.
* If DAGs are not visible in UI, verify:

  * `AIRFLOW_HOME`
  * `AIRFLOW__CORE__DAGS_FOLDER`

---

## 10. Future Enhancements

* Parameterize data volumes & webhook URLs via Airflow Variables.
* Add **alerting** via email/webhooks on ETL failures.
* Add a **data validation task** post-load (row counts, schema checks).
* Automate **log pruning** with a dedicated Airflow DAG.

---

## 11. End-to-End Workflow Summary

1. **Synthetic Stream (Airflow DAG `synth_stream_5min`)** → Inserts operational data into `ecom_db`.
2. **ETL Pipeline (Airflow DAG `ecom_etl_10min`)** → Cleans & loads into `ecom_dwh`.
3. **Analytics/BI** → Runs directly on `ecom_dwh` (Power BI, Tableau, etc.).

This setup simulates a real **data engineering ecosystem** with orchestration, streaming, transformation, and warehousing.

---
