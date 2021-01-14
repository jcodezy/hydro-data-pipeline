# hydro-data-pipeline
My first data pipeline, using airflow, gcp and streamlit 

### Languages/Tools used:
1. Python
2. Airflow
3. Google Cloud Platform (Google Cloud Storage & BigQuery) 
4. Streamlit for data visualization (work in progress)
5. SQL

### Introduction 
#### This small data pipeline uses python and the airflow orchestration tool to run these tasks once per day. My electricity provider does not provide real-time data; instead only providing yesterday's data on the current day.
##### DAG tasks: 
- download a csv of my electricity data from the previous day (using Selenium) 
- clean downloaded csv data (ie. remove PII) and downloads as parquet file 
- use FileToGoogleCloudStorage operator to move the local file to a GCS landing bucket 
- use GoogleCloudStorageToBigQuery operator to move from GCS landing bucket into BigQuery table

#### Coming next:
- Create alternate tables / views in BigQuery for further analytical processing
- Query BigQuery tables and use streamlit to create data visualization dashboard 
