from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

class BigQueryInsertJobOperatorNoTemplate(BigQueryInsertJobOperator):
    template_fields = []  # prevent Jinja from trying to template ANY fields

with DAG(
    dag_id='run_bq_sql_from_gcs_fixed',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['bigquery'],
) as dag:
    
    execute_gcs_bqsql = BigQueryInsertJobOperator(
        task_id='execute_bqsql_from_gcs_task',
        configuration={
            "query": {
                "queryUri": "gs://firstworkflow/ddl/Temperature_DDL.sql",
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        gcp_conn_id='google_cloud_default',  # Or your specific GCP connection ID
    )

   