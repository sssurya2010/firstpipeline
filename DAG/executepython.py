from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 21),
    'retries': 1,
}

with DAG(
    dag_id='run_python_from_gcs',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['example'],
) as dag:

    download_and_run_script = BashOperator(
        task_id='download_and_run_script',
        bash_command="""
        gsutil cp gs://firstworkflow/code/weather.py
        python3 /tmp/weather.py
        """
    )

    download_and_run_script
