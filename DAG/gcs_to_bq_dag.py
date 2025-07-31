from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

def move_to_processed_func(execution_date, **context):
    gcs_hook = GCSHook()
    src_bucket = 'firstworkflow'
    src_object = 'output/country_wise_temp.csv'
    dest_object = f"output/processed/country_wise_temp_{execution_date.strftime('%Y%m%d_%H%M%S')}.csv"
    gcs_hook.copy(src_bucket, src_object, src_bucket, dest_object)
    gcs_hook.delete(src_bucket, src_object)

with DAG(
    'gcs_to_bq_dag',
    default_args=default_args,
    description='A DAG to load CSV data from GCS to BigQuery and move to processed folder',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    load_csv_to_bq = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='firstworkflow',
        source_objects=['output/country_wise_temp.csv'],
        destination_project_dataset_table='gcs-project-461318.firstflow.TemperatureTable',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND',
    )

    move_to_processed = PythonOperator(
        task_id='move_to_processed',
        python_callable=move_to_processed_func,
        op_kwargs={'execution_date': '{{ execution_date }}'},
    )

    load_csv_to_bq >> move_to_processed