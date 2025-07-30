from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime

# Disable templating to avoid Jinja trying to load gs:// as local template
class BigQueryInsertJobOperatorNoTemplate(BigQueryInsertJobOperator):
    template_fields = []

default_args = {
    'retries': 0
}


with DAG(
    dag_id='run_bq_sql_from_gcs_fixed',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['bigquery'],
) as dag:

    # ...existing code...


    def read_sql_from_gcs(bucket_name, object_name, gcp_conn_id='google_cloud_default'):
        hook = GCSHook(gcp_conn_id=gcp_conn_id)
        return hook.download(bucket_name, object_name).decode('utf-8')

    bucket = 'firstworkflow'
    object_name = 'ddl/Temprature_DDL.sql'
    ddl_query = read_sql_from_gcs(bucket, object_name)

    execute_gcs_bqsql = BigQueryInsertJobOperatorNoTemplate(
        task_id='execute_bqsql_from_gcs_task',
        configuration={
            "query": {
                "query": ddl_query,
                "useLegacySql": False
            }
        },
        gcp_conn_id='google_gcs_project_conn',
        location='US'
    )
# ...existing code...

execute_gcs_bqsql
