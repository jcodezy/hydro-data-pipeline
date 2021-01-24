import os
import glob
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import sys
sys.path.append(os.getenv('SELENIUM_SCRIPT_FILEPATH')) 
from selenium_script import download_csv_raw
from datetime import datetime,timedelta
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from google.cloud import storage, bigquery
from csv_cleaner_func import csv_cleaner
    
DATA_DOWNLOAD_FILEPATH = os.getenv('DATA_DOWNLOAD_FILEPATH')
HYDRO_DATA_PROJECT_ID=os.getenv('HYDRO_DATA_PROJECT_ID')
HYDRO_DATA_LANDING_BUCKET = os.getenv('HYDRO_DATA_LANDING_BUCKET')
HYDRO_STAGING_DATASET = os.getenv('HYDRO_STAGING_DATASET')
GOOGLE_CLOUD_STORAGE_CONN_ID = os.getenv('GOOGLE_CLOUD_STORAGE_CONN_ID')
BIGQUERY_CONN_ID = os.getenv('BIGQUERY_CONN_ID')
BIGQUERY_DAILY_HYDRO_TABLE = os.getenv('BIGQUERY_DAILY_HDYRO_TABLE')

def check_variables():
    if all(arg is not None for arg in [DATA_DOWNLOAD_FILEPATH,HYDRO_DATA_PROJECT_ID,HYDRO_DATA_LANDING_BUCKET,HYDRO_STAGING_DATASET]):
        print("Variables are set")
check_variables() 

default_args = {
    'owner': 'JC U',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_retry': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': 3,
    'provide_context':True
}

with DAG(
    'dag',
    schedule_interval='0 8 * * *',
    start_date=days_ago(1),
    catchup=False) as dag:

    download_yesterdays_csv = PythonOperator(
        task_id='download_yesterdays_csv',
        python_callable=download_csv_raw #selenium script
    )
    
    clean_csv_before_upload = PythonOperator(
        task_id='clean_csv_before_upload',
        python_callable=csv_cleaner,
        do_xcom_push=True
    )

    upload_file_to_gcs = FileToGoogleCloudStorageOperator(
        task_id='upload_file_to_gcs',
        src=f'{DATA_DOWNLOAD_FILEPATH}' + '''{{ ti.xcom_pull(task_ids='clean_csv_before_upload') }}''',
        dst='''{{ ti.xcom_pull(task_ids='clean_csv_before_upload') }}''',
        bucket=HYDRO_DATA_LANDING_BUCKET,
        google_cloud_storage_conn_id=GOOGLE_CLOUD_STORAGE_CONN_ID,
        mime_type='parquet'
    )

    # schema for bigquery table 
    schema_fields = [
        {
            "name": "interval_start_date_time",
            "type": "TIMESTAMP",
            "mode": "REQUIRED"
        },
        {
            "name": "net_consumption_kwh",
            "type": "FLOAT",
            "mode": "NULLABLE"
        }
    ]

    gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=HYDRO_DATA_LANDING_BUCKET,
        source_objects=['*.parquet'], 
        source_format='PARQUET',
        destination_project_dataset_table=f'{HYDRO_DATA_PROJECT_ID}:{HYDRO_STAGING_DATASET}.{BIGQUERY_DAILY_HYDRO_TABLE}',
        schema_fields=schema_fields, 
        skip_leading_rows=1,
        bigquery_conn_id=BIGQUERY_CONN_ID,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE', 
    )

download_yesterdays_csv >> clean_csv_before_upload >> upload_file_to_gcs >> gcs_to_bq