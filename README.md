# hydro-data-pipeline
My first data pipeline, using airflow, gcp and streamlit 

### Languages/Tools used:
- Python
- Airflow
- GCP (Google Cloud Storage, BigQuery, DataProc) 
- PySpark
- SQL

### Introduction 
#### This small data pipeline uses python and the airflow orchestration tool to run these tasks once per day. My electricity provider does not provide real-time data; instead only providing yesterday's data on the current day.
##### DAG tasks
Runs once a day: 
- download a csv of my electricity data from the previous day (using Selenium) 
- clean downloaded csv data (ie. remove PII) and downloads as parquet file 
- use FileToGoogleCloudStorage operator to move the local file to a GCS landing bucket 
- use GoogleCloudStorageToBigQuery operator to move from GCS landing bucket into BigQuery table

##### backfill.py 
I created a backfill.py job to download historical data up until the daily DAG starts. I then cleaned the historical dataset, convert CSV to parquet to make the file more compact and maintain data types. After cleaning, I upload to a google cloud storage landing bucket using the cloud storage API. 

#### TODO:
- Create a separate cleaner dag that deletes the files already in gcs off of my local machine to save space and also..
- Create a task that ensures that every daily file is uploaded to gcs; if not, run selenium script for missing days and upload to gcs 
- Create alternate tables / views in BigQuery for further analytical processing
- Query BigQuery tables and use streamlit to create data visualization dashboard 

### Detailed Workflow 
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