from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

local_tz = pendulum.timezone("Asia/Kolkata")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SYNTH_SCRIPT = "/media/softsuave/ef660aa7-82af-4e74-a314-ff3c6b242904/DataEngineering/Apache_Airflow/scripts/synth/run_synth_stream.sh"
ETL_SCRIPT = "/media/softsuave/ef660aa7-82af-4e74-a314-ff3c6b242904/DataEngineering/Apache_Airflow/scripts/etl/run_etl.sh"

# -----------------------------------------
# DAG 1: Synthetic stream every 5 minutes starting at 12:35
# -----------------------------------------
with DAG(
    dag_id="synth_stream_5min",
    description="Generate synthetic e-commerce data every 5 minutes, starting at 12:35",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2025, 9, 8, 12, 35, 0, tz=local_tz),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1,
) as synth_dag:
    run_synth_stream = BashOperator(
        task_id="run_synth_stream",
        bash_command=f"bash {SYNTH_SCRIPT} | cat",
        env={
            "PATH": "/media/softsuave/ef660aa7-82af-4e74-a314-ff3c6b242904/DataEngineering/Apache_Airflow/airflow_venv/bin:" +
                    "{{ var.value.get('PATH', '/usr/bin:/bin') }}"
        },
    )

# -----------------------------------------
# DAG 2: ETL pipeline every 10 minutes starting at 12:40
# -----------------------------------------
with DAG(
    dag_id="ecom_etl_10min",
    description="Run ETL pipeline every 10 minutes, starting at 12:40",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2025, 9, 8, 12, 40, 0, tz=local_tz),
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    max_active_runs=1,
) as etl_dag:
    run_etl = BashOperator(
        task_id="run_etl_pipeline",
        bash_command=f"bash {ETL_SCRIPT} | cat",
        env={
            "PATH": "/media/softsuave/ef660aa7-82af-4e74-a314-ff3c6b242904/DataEngineering/Apache_Airflow/airflow_venv/bin:" +
                    "{{ var.value.get('PATH', '/usr/bin:/bin') }}"
        },
    )