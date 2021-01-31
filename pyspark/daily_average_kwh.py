#!/usr/bin/python
"""
To be run in the pyspark_processing DAG on google dataproc.
"""

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('daily-analytics-avg-kwh') \
    .getOrCreate() 

BUCKET = 'hydro-data-spark-bucket'
spark.conf.set('temporaryGcsBucket', BUCKET)

# Create a temporary view from existing bq table
daily_hydro_history = spark.read.format('bigquery') \
    .option('table', 'jc-hydro-data:staging.daily_hydro_data') \
    .load()
daily_hydro_history.createOrReplaceTempView('daily_hydro_history')

# Perform avg kwh calculation AND group by day 
avg_kwh_per_day = spark.sql("""
SELECT 
    CAST(interval_start_date_time as DATE) as interval_day, 
    AVG(net_consumption_kwh) as avg_kwh 
FROM 
    daily_hydro_history 
GROUP BY 1
""")
avg_kwh_per_day.show()
avg_kwh_per_day.printSchema()

# Save results to new BQ table 
avg_kwh_per_day.write.format('bigquery') \
    .option('table', 'staging.daily_avg_kwh') \
    .mode('overwrite') \
    .save()
