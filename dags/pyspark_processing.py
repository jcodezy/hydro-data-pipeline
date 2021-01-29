"""
This DAG is responsible for running analytical processing 
from data that's already loaded to BQ. 

Steps: 
1. Create dataproc cluster
2. Use to run DataProcPySparkOperator the file file pyspark/daily_average.py
3. Write to BQ
4. Delete cluster when finished 
"""

import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)

HYDRO_DATA_PROJECT_ID = os.getenv("HYDRO_DATA_PROJECT_ID")
SPARK_BUCKET = os.getenv("SPARK_BUCKET")

default_arguments = {"owner": "JC U", "start_date": days_ago(1)}

with DAG(
    "bigquery_data_analytics_processing",
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
        zone="us-west-1",
        auto_delete_time=60,  # realistically shouldn't take any longer than 60s
    )

    calculate_daily_average_kwh = DataProcPySparkOperator(
        task_id="calculate_daily_average_kwh",
        main=f"gs://{SPARK_BUCKET}/pyspark/daily_average_kwh.py",
        dataproc_pyspark_jars="gs://spark-lib/bigquery/spark-bigquery-latest.jar",
    )

    delete_cluster = DataprocClusterDeleteOperator(
        task_id="delete_cluster",
        project_id=HYDRO_DATA_PROJECT_ID,
        cluster_name="spark-cluster-{{ ds_nodash }}",
        trigger_rule="all_done",
    )

create_cluster >> calculate_daily_average_kwh >> delete_cluster
