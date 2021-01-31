"""
This DAG is responsible for running a pyspark job on dataproc
from BigQuery table and writing/appending to a new table. 

Steps: 
1. Create dataproc cluster
2. Use to run DataProcPySparkOperator the file file pyspark/daily_average.py
3. Write to BQ
4. Delete cluster when finished 
"""

import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)

HYDRO_DATA_PROJECT_ID = os.getenv("HYDRO_DATA_PROJECT_ID")
SPARK_BUCKET = os.getenv("SPARK_BUCKET")
GOOGLE_CLOUD_STORAGE_CONN_ID = os.getenv("GOOGLE_CLOUD_STORAGE_CONN_ID")
default_arguments = {"owner": "JC U", "start_date": days_ago(1)}

with DAG(
    "dataproc_analytics_processing",
    schedule_interval="0 8 * * *",
    catchup=False,
    default_args=default_arguments,
) as dag:

    create_cluster = DataprocClusterCreateOperator(
        task_id="create_cluster",
        project_id=HYDRO_DATA_PROJECT_ID,
        cluster_name="spark-cluster-{{ ds_nodash }}",  # spark-cluster-YYYMMDD
        num_workers=2,
        storage_bucket=SPARK_BUCKET,
        region="us-west1",
        zone="us-west1-a",
        idle_delete_ttl=300,  #  5 mins is the min. value
        gcp_conn_id=GOOGLE_CLOUD_STORAGE_CONN_ID,
    )

    calculate_daily_average_kwh = DataProcPySparkOperator(
        task_id="calculate_daily_average_kwh",
        main=f"gs://{SPARK_BUCKET}/pyspark/daily_average_kwh.py",
        cluster_name="spark-cluster-{{ ds_nodash }}",
        dataproc_pyspark_jars="gs://spark-lib/bigquery/spark-bigquery-latest.jar",
        gcp_conn_id=GOOGLE_CLOUD_STORAGE_CONN_ID,
        region="us-west1",
    )

    calculate_daily_sum_kwh = DataProcPySparkOperator(
        task_id="calculate_daily_sum_kwh",
        main=f"gs://{SPARK_BUCKET}/pyspark/daily_sum_kwh.py",
        cluster_name="spark-cluster-{{ ds_nodash }}",
        dataproc_pyspark_jars="gs://spark-lib/bigquery/spark-bigquery-latest.jar",
        gcp_conn_id=GOOGLE_CLOUD_STORAGE_CONN_ID,
        region="us-west1"
    )

    delete_cluster = DataprocClusterDeleteOperator(
        task_id="delete_cluster",
        project_id=HYDRO_DATA_PROJECT_ID,
        cluster_name="spark-cluster-{{ ds_nodash }}",
        trigger_rule="all_done",  # deletes cluster regardless when all tasks are done
        gcp_conn_id=GOOGLE_CLOUD_STORAGE_CONN_ID,
        region="us-west1",
    )

create_cluster >> [calculate_daily_average_kwh, calculate_daily_sum_kwh] >> delete_cluster
