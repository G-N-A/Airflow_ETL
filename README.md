E-commerce Data Platform

A scalable data engineering platform for simulating, processing, and analyzing e-commerce data using Apache Airflow, PySpark, and SQL Server.
🚀 Project Overview

This project is a complete data engineering solution that uses a modern data stack to handle an end-to-end data pipeline. It is built with a focus on automation, scalability, and maintainability.
Core Features

    Data Simulation: Generates realistic e-commerce data, simulating live customer interactions and transactions.

    Automated ETL: A robust PySpark pipeline cleanses, transforms, and loads raw data into a data warehouse.

    Workflow Orchestration: Apache Airflow automates the entire process, providing a rich UI for monitoring and management.

🔧 Technology Stack

Technology
	

Role

Apache Airflow
	

The primary orchestration engine, managing all workflows and dependencies.

PySpark
	

The distributed processing engine for all data transformation (ETL) tasks.

Microsoft SQL Server
	

Serves as both the source transactional database (ecom_db) and the target data warehouse (ecom_dwh).

Python 3.x
	

The core programming language for scripting and data generation.

Bash Scripts
	

Provides simple, repeatable entry points for running jobs.
📂 Folder Structure

The project is organized in a logical and easy-to-navigate manner.

project-root/
│
├── dags/                # All Airflow DAG definitions
│   └── ecom_schedules.py
│
├── src/                 # Core Python source code
│   ├── synth_ecom_mssql_stream.py
│   └── ecom_etl_pipeline.py
│
├── scripts/             # Wrapper scripts for running jobs
│   ├── etl/
│   │   └── run_etl.sh
│   └── synth/
│       └── run_synth_stream.sh
│
├── libs/jars/           # External libraries (e.g., JDBC driver)
│   └── mssql-jdbc-*.jar
│
├── logs/                # Centralized logs for all processes
└── docs/                # Project documentation

💧 End-to-End Data Flow

The data moves through a clear, automated pipeline:

    Generation: The synth_ecom_mssql_stream.py script generates sample data and inserts it into the ecom_db (OLTP layer).

    Orchestration: Airflow's synth_stream_5min DAG triggers the data generation process every 5 minutes.

    ETL: The ecom_etl_10min DAG runs the ecom_etl_pipeline.py script, which extracts the data, applies transformations, and cleans it.

    Loading: The cleaned data is loaded into the ecom_dwh (data warehouse) where it's ready for analytics and business intelligence.

▶️ Quickstart & Operations

Follow these steps to get the project running.
Run Data Jobs

Use these simple wrapper scripts to run the core data jobs from the command line.

Run Synthetic Stream:

bash scripts/synth/run_synth_stream.sh

Run ETL Pipeline:

bash scripts/etl/run_etl.sh

Airflow Management

These commands help you manage the Airflow webserver and scheduler.

Start Services:

bash scripts/airflow/run_airflow.sh

Unpause a DAG:

airflow dags unpause <dag_name>

If you have any questions or want to dive deeper into a specific part of the project, just let me know!
