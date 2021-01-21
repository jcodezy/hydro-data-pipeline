import os
import pandas as pd 
import glob 

from datetime import datetime 
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from google.cloud import bigquery 
from dags.csv_cleaner_func import check_dataframe_for_na_values
from src.selenium_script import download_csv_raw
from google.cloud import storage
from time import strptime 

DATA_DOWNLOAD_FILEPATH = os.getenv('BC_HYDRO_DOWNLOAD_PATH')
HYDRO_DATA_PROJECT_ID = os.getenv('HYDRO_DATA_PROJECT_ID')
HYDRO_DATA_LANDING_BUCKET = os.getenv('HYDRO_DATA_LANDING_BUCKET')

# variables for download_csv_raw
from_date_day, from_date_month, from_date_year = '03', 'Aug', '2020' # date when hydro started
to_date_day, to_date_month, to_date_year = '10', 'Jan', '2021' # daily DAG tasks started on Jan 11th 2021 

# clean historical data set 
def clean_historical_dataset(historical_dataset_to_clean, from_date_day, from_date_month, from_date_year, to_date_day, to_date_month, to_date_year):

    if historical_dataset_to_clean:
        df = pd.read_csv(
            historical_dataset_to_clean,
            usecols=['Interval Start Date/Time', 'Net Consumption (kWh)']
        )
        size = df.shape
        df = df.rename(columns={'Interval Start Date/Time':'interval_start_date_time','Net Consumption (kWh)':'net_consumption_kwh'})
        df['interval_start_date_time'] = pd.to_datetime(df['interval_start_date_time'])

        if df['net_consumption_kwh'].dtype not in ['float64', 'float']: # some kwh are "n/a" 
            try:
                df['net_consumption_kwh'].fillna(df['net_consumption_kwh'].mean(), inplace=True)
                df.astype({'net_consumption_kwh': 'float64'})
            except:
                print("Found objects but had trouble converting object to float")
                return

        # converting 'Aug' to 08 to match prefix naming in gcs; ie. '20200803' instead of '2020Aug..'
        from_date_month, to_date_month = "0" + str(strptime(from_date_month, '%b').tm_mon), "0" + str(strptime(to_date_month, '%b').tm_mon) 
        # convert csv file to .parquet file
        parquet_file_title = f"{from_date_year}{from_date_month}{from_date_day}-{to_date_year}{to_date_month}{to_date_day}"
        df.to_parquet(f"{DATA_DOWNLOAD_FILEPATH}/{parquet_file_title}.parquet", index=False)
        print(f"Sucessfully cleaned csv and converted to parquet: {parquet_file_title}.parquet")
        
        # delete historical dataset from local dir
        try:
            os.remove(f"{historical_dataset_to_clean}")
        except:
            print(f"Could not remove {historical_dataset_to_clean}")
        
        return f"{DATA_DOWNLOAD_FILEPATH}/{parquet_file_title}.parquet"

    else:
        print(f"Did not find dataset to clean.")
        return 


def upload_historical_data_to_gcs(destination_bucket, source_file_name, destination_blob_name):
    try:
        storage_client = storage.Client(project='jc-hydro-data')
        bucket = storage_client.bucket(destination_bucket)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name, content_type='parquet')
        print(f"File {source_file_name} uploaded to {destination_bucket}.")

    except:
        print("Could not send file to google cloud storage.")
        return

historical_dataset_to_clean = download_csv_raw(
    from_date_day=from_date_day, 
    from_date_month=from_date_month, 
    from_date_year=from_date_year, 
    to_date_day=to_date_day, 
    to_date_month=to_date_month, 
    to_date_year=to_date_year)

source_file_name = clean_historical_dataset(
    historical_dataset_to_clean=historical_dataset_to_clean, 
    from_date_day=from_date_day,
    from_date_month=from_date_month,
    from_date_year=from_date_year,
    to_date_day=to_date_day,
    to_date_month=to_date_month,
    to_date_year=to_date_year
)

upload_historical_data_to_gcs(
    destination_bucket=HYDRO_DATA_LANDING_BUCKET,
    source_file_name=source_file_name,
    destination_blob_name=source_file_name.split("/")[5]
)