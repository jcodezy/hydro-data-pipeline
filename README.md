# hydro-data-pipeline
My data pipeline, using airflow, gcp, spark and streamlit. 

### Introduction 
#### This data pipeline downloads, cleans and uploads data to Google Cloud and then completes a few analytical jobs before getting put into a streamlit chart. 
The streamlit chart once the jobs are complete: 
![daily_sum_kwh_chart](https://github.com/jcodezy/hydro-data-pipeline/blob/master/markdown_assets/chart_and_table_1.png)

### Languages/Tools used:
- Python
- Airflow
- GCP (Google Cloud Storage, BigQuery, DataProc, Compute) 
- PySpark
- SQL

##### [backfill.py](https://github.com/jcodezy/hydro-data-pipeline/blob/master/backfill.py) 
I created a backfill.py job to download historical data up until the daily DAG starts. I then cleaned the historical dataset, convert CSV to parquet to make the file more compact and maintain data types. After cleaning, I upload to a google cloud storage landing bucket using the cloud storage API. 

#### DAG: [load_gcs_and_bq.py](https://github.com/jcodezy/hydro-data-pipeline/blob/master/dags/load_gcs_and_bq.py) 
Runs once a day: 
- download a csv of my electricity data from the previous day (using Selenium) 
- clean downloaded csv data (ie. remove PII) and downloads as parquet file 
- use FileToGoogleCloudStorage operator to move the local file to a GCS landing bucket 
- use GoogleCloudStorageToBigQuery operator to move from GCS landing bucket into BigQuery table

**Detailed load_gcs_and_bq workflow**
1. Download dataset using selenium script. This is what a sample of the data looks like with no cleaning. 
![Raw data sample](https://github.com/jcodezy/hydro-data-pipeline/blob/master/markdown_assets/raw_csv_download_sample.png)

2. Data cleaning steps: 
    - remove personal identifying information 
    - fix column names; removed spaces and replaced with underscores and used lowercase to match bigquery column name constraints 
    - check that kwh column is of float type
        - if column is found to have NaN values, fill NaN values with column (kwh) average (mean imputation)
    - save to parquet file 

3. Upload cleaned file to google cloud storage using the FileToGoogleCloudStorage operator. Below is the landing bucket with historical data (backfill.py) as the first entry and then the daily entries thereafter.  
![landing bucket](https://github.com/jcodezy/hydro-data-pipeline/blob/master/markdown_assets/gcs_landing_bucket_w_historical.png)

4. Even though bigquery infers a schema based on the parquet file, I decided to define the schema anyway as bigquery was inferring a BIGINT datatype for the datetime column.
5. Transfer google cloud storage files to bigquery using 'WRITE_TRUNCATE' write_disposition; as bigquery has no fees on loading a bq table, I figured it was good to use WRITE_TRUNCATE to be idempotent. 
![bigquery schema](https://github.com/jcodezy/hydro-data-pipeline/blob/master/markdown_assets/bigquery_schema.png)

DAG: [pyspark_processing](https://github.com/jcodezy/hydro-data-pipeline/blob/master/dags/pyspark_processing.py)  
![pyspark dag](https://github.com/jcodezy/hydro-data-pipeline/blob/master/markdown_assets/pyspark_dag.png)

This DAG runs separately and simulates two "heavy" pyspark jobs running on GCP's DataProc service:
- read from bigquery table
- perform analytics 
- write to new bigquery table 
- delete dataproc cluster

#### Future Improvements / TODO:
- Implement more tests throughout 
- Create a DAG that cleans local dir (ie. delete uploaded files if they are on GCS)