import os
from datetime import datetime 
import pandas as pd 
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from google.cloud import bigquery 
from dags.csv_cleaner_func import check_dataframe_for_na_values
from src.selenium_script import download_csv_raw

HYDRO_DATA_PROJECT_ID = os.getenv('HYDRO_DATA_PROJECT_ID')
STAGING_DATASET = os.getenv('STAGING_DATASET')
DAILY_HYDRO_DATA_TABLE = os.getenv('DAILY_HYDRO_DATA_TABLE')
HISTORICAL_DATASET_FILE = os.getenv('HISTORICAL_DATASET_FILE')
DATA_DOWNLOAD_FILEPATH = os.getenv('BC_HYDRO_DOWNLOAD_PATH')

# variables for  download_csv_raw
from_date_day, from_date_month, from_date_year = '03', 'Aug', '2020' # date when hydro started
to_date_day, to_date_month, to_date_year = '10', 'Jan', '2021' # daily DAG tasks started on Jan 11th 2021 

# clean historical data set 
def clean_historical_dataset(historical_dataset_to_clean=None):

    if historical_dataset_to_clean is not None:
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

        parquet_file_title = f"{from_date_day}{from_date_month}{from_date_year}-{to_date_day}{to_date_month}{to_date_year}"
        df.to_parquet(f"{DATA_DOWNLOAD_FILEPATH}/{parquet_file_title}.parquet", index=False)
        print(f"Sucessfully downloaded: {parquet_file_title}.parquet")
        return f"{DATA_DOWNLOAD_FILEPATH}/{parquet_file_title}.parquet"

    else:
        print(f"Did not find dataset to clean.")
        return 

historical_dataset_to_clean = download_csv_raw(
    from_date_day=from_date_day, 
    from_date_month=from_date_month, 
    from_date_year=from_date_year, 
    to_date_day=to_date_day, 
    to_date_month=to_date_month, 
    to_date_year=to_date_year)

clean_historical_dataset(historical_dataset_to_clean)
