#!/usr/bin/python
"""
To be run in the pyspark_processing DAG on google dataproc.
"""
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('bigquery-daily-avg-kwh') \
    .getOrCreate() 

HYDRO_DATA_PROJECT_ID = os.getenv('HYDRO_DATA_PROJECT_ID')
STAGING_DATASET = os.getenv('HYDRO_STAGING_DATASET')
BUCKET = os.getenv('SPARK_BUCKET')
spark.conf.set('temporaryGcsBucket', BUCKET)

# Create a temporary view from existing bq table
daily_hydro_history = spark.read.format('bigquery') \
    .option('table', f'{jHYDRO_DATA_PROJECT_ID}.{STAGING_DATASET}.daily_hydro_data') \
    .load()
daily_hydro_history.createOrReplaceTempView('daily_hydro_history')

# Perform avg kwh calculation AND group by day 
avg_kwh_per_day = spark.sql("""
SELECT 
    CAST(interval_start_date_time as DATE) as interval_day, 
    AVG(net_consumption_kwh) as avg_kwh 
FROM daily_hydro_history 
GROUP BY 1
""")
avg_kwh_per_day.show()
avg_kwh_per_day.printSchema()

# Save results to new BQ table 
avg_kwh_per_day.write.format('bigquery') \
    .option('table', 'staging.daily_avg_kwh') \
    .mode('overwrite') \
    .save()