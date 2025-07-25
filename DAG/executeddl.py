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

    run_sql_from_gcs = BigQueryInsertJobOperatorNoTemplate(
        task_id='run_sql_file',
        configuration={
            "query": {
                "query": "",
                "queryParameters": [],
                "useLegacySql": False,
                "sourceUris": ["gs://firstworkflow/ddl/Temperature_DDL.sql"]
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",
    )

    run_sql_from_gcs
