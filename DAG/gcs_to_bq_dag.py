from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG(
    'gcs_to_bq_dag',
    default_args=default_args,
    description='A DAG to load CSV data from GCS to BigQuery',
    schedule_interval='@daily',
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

    move_to_processed = GCSToGCSOperator(
        task_id='move_to_processed',
        source_bucket='firstworkflow',
        source_object='output/country_wise_temp.csv',
        destination_bucket='firstworkflow',
        destination_object='output/processed/country_wise_temp_{{ ds_nodash }}_{{ ts_nodash }}.csv',
        move_object=True,
    )

    load_csv_to_bq >> move_to_processed