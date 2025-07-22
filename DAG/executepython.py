from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

def run_script_from_gcs():
    subprocess.run([
        "gsutil", "cp", "gs://firstworkflow/code/weather.py", "/tmp/weather.py"
    ], check=True)
    subprocess.run(["python3", "/tmp/your_script.py"], check=True)

with DAG(
    dag_id='run_python_from_gcs_python_operator',
    start_date=datetime(2025, 7, 21),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_task = PythonOperator(
        task_id='run_python',
        python_callable=run_script_from_gcs,
    )

    run_task
