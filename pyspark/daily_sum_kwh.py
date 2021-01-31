#!/usr/bin/python
"""
Get sum of kwh for each day; this pyspark job to be run on dataproc. 
"""
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('daily-analytics-sum-kwh') \
    .getOrCreate()

BUCKET = 'hydro-data-spark-bucket'
spark.conf.set('temporaryGcsBucket', BUCKET)

daily_hydro_history = spark.read.format('bigquery') \
    .option('table', 'jc-hydro-data:staging.daily_hydro_data') \
    .load()
daily_hydro_history.createOrReplaceTempView('daily_hydro_history')

sum_kwh_per_day = spark.sql("""
SELECT
    CAST(interval_start_date_time as DATE) as interval_day,
    ROUND(SUM(net_consumption_kwh),2) as sum_kwh
FROM 
    daily_hydro_history
GROUP BY 1
""")
sum_kwh_per_day.show()
sum_kwh_per_day.printSchema()
sum_kwh_per_day.write.format('bigquery') \
    .option('table', 'staging.daily_sum_kwh') \
    .mode('overwrite') \
    .save()