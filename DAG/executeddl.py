from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

with DAG(
    dag_id='run_bq_sql_from_gcs',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['bigquery'],
) as dag:

    run_sql_from_gcs = BigQueryInsertJobOperator(
        task_id='run_sql_file',
        configuration={
            "query": {
                "query": None,
                "queryParameters": [],
                "useLegacySql": False,
                "sourceUris": ["gs://firstworkflow/ddl/Temperature_DDL.sql"]
            }
        },
        location="US",  # Replace with your region if needed
        gcp_conn_id="google_cloud_default",
    )

    

    
