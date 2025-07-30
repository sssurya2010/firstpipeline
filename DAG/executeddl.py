from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

# Disable templating to avoid Jinja trying to load gs:// as local template
class BigQueryInsertJobOperatorNoTemplate(BigQueryInsertJobOperator):
    template_fields = []

with DAG(
    dag_id='run_bq_sql_from_gcs_fixed',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['bigquery'],
) as dag:

      execute_gcs_bqsql = BigQueryInsertJobOperatorNoTemplate(
        task_id='execute_bqsql_from_gcs_task',
        configuration={
            "query": {
                "query": "SELECT 1",  # Dummy query to satisfy required parameter
                "sourceUris": ["gs://firstworkflow/ddl/Temprature_DDL.sql"],
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE"
            }
        },
        gcp_conn_id='google_cloud_default',
        location='US'
    )

execute_gcs_bqsql
