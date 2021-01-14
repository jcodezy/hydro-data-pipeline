import os
import sys
import glob
import pandas as pd 
from datetime import datetime,timedelta
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


DATA_DOWNLOAD_FILEPATH = os.getenv('DATA_DOWNLOAD_FILEPATH')

def check_dataframe_for_na_values(column):
    if column.dtype != 'float64':
        print(f"Data in {column} is not of float type as expected. Check data source.")
        return

def csv_cleaner():
    if not DATA_DOWNLOAD_FILEPATH:
        print("Download filepath is not defined")
    fpath = f'{DATA_DOWNLOAD_FILEPATH}*.csv'

    csv_to_clean = glob.glob(fpath)[0]
    if not csv_to_clean:
        print(f"Did not find CSV to clean in {fpath}")
        return 
    print("The raw CSV file to be cleaned: ", csv_to_clean)

    # clean & transformations 
    df = pd.read_csv(csv_to_clean, usecols=['Interval Start Date/Time', 'Net Consumption (kWh)']) # deletes account number & PII 
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d") 
    df = df[df["Interval Start Date/Time"].str.startswith(f"{yesterday}")].reset_index(drop=True)
    df["Interval Start Date/Time"] = pd.to_datetime(df["Interval Start Date/Time"], infer_datetime_format=True, yearfirst=True, format="%Y-%m-%d %H:%M")
    df = df.rename(columns={"Interval Start Date/Time": "interval_start_date_time", "Net Consumption (kWh)": "net_consumption_kwh"})
    print(df.dtypes)

    # create a check to see if the rows aren't all "N/A" as a result of data not being available from bc hydro or data is corrupted
    check_dataframe_for_na_values(df['net_consumption_kwh']) 

    # save to parquet format to keep datetime/timestamp data types and let BQ infer schema 
    to_parquet_date = yesterday.replace("-", "") # e.g. '20201228' 
    df.to_parquet(f"{DATA_DOWNLOAD_FILEPATH}/{to_parquet_date}.parquet", index=False)

    # delete old raw file from dir 
    files = glob.glob(f"{DATA_DOWNLOAD_FILEPATH}*.csv") 
    for f in files:
        if f == csv_to_clean:
            os.remove(f)
            print("Sucessfully removed: ", f)
        else:
            print("Cant find the file to delete. Found this file instead: ", f) 
    return f"{to_parquet_date}.parquet" # for xcom downstream 