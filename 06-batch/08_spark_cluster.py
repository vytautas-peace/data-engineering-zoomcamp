#!/usr/bin/env python
# coding: utf-8
"""
HOW TO RUN:

LOCALLY:

"$SPARK_HOME/sbin/start-master.sh"
"$SPARK_HOME/sbin/start-worker.sh"

URL="spark://de-zoomcamp.europe-west1-b.c.de-zoomcamp-489904.internal:7077" # From Spark Master URL

park-submit \
    --master="${URL}" \
    06_spark_local.py \
    --input_green=data/raw/green/2020/*/ \
    --input_yellow=data/raw/yellow/2020/*/ \
    --lookup=data/raw/
    --output=data/report/report-2020

 "$SPARK_HOME/sbin/stop-all.sh"

================================

GCLOUD:

gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region europe-west2 \
    gs://nyc-tlc-data-lake/code/08_spark_cluster.py \
    -- \
    --input_green=gs://nyc-tlc-data-lake/pq/green/2021/*/ \
    --input_yellow=gs://nyc-tlc-data-lake/pq/yellow/2021/*/ \
    --lookup=gs://nyc-tlc-data-lake/ \
    --output=gs://nyc-tlc-data-lake/output-2021/  

"""

import time
import argparse
import pyspark
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--lookup', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
lookup = args.lookup
output = args.output


start_time = time.perf_counter()
spark = None

try:
    # Initiate session.
    spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate()

    # Run this right after creating your 'spark' session
    spark.sparkContext.setLogLevel("ERROR")



    # Read green data, normalise column names, register a table for running SQL.
    df_green = spark.read.parquet(input_green)
    df_green = df_green \
        .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
    df_green.createOrReplaceTempView('green')

    # Define Spark query.
    df_green_revenue = spark.sql("""
    select
        date_trunc('hour', pickup_datetime) as hour,
        PULocationID as zone_id,
        
        sum(total_amount) as green_amount,
        count(1) as green_number_records
    from
        green
    where
        pickup_datetime >= '2019-01-01 00:00:00'
    group by
        1, 2
    order by
        1, 2;
    """)

    # Write results to file.
    df_green_revenue.coalesce(1).write.parquet(f'{output}/green', mode='overwrite')

    # Read yellow data, normalise column names, register a table for running SQL.
    df_yellow = spark.read.parquet(input_yellow)
    df_yellow = df_yellow \
        .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
    df_yellow.createOrReplaceTempView('yellow')

    # Define Spark query.
    df_yellow_revenue = spark.sql("""
    select
        date_trunc('hour', pickup_datetime) as hour,
        PULocationID as zone_id,
        
        sum(total_amount) as yellow_amount,
        count(1) as yellow_number_records
    from
        yellow
    where
        pickup_datetime >= '2019-01-01 00:00:00'
    group by
        1, 2
    order by
        1, 2;
    """)

    # Write results to file.
    df_yellow_revenue.coalesce(1).write.parquet(f'{output}/yellow', mode='overwrite')

    # Outer join & write to file.
    df_join = df_green_revenue.join(df_yellow_revenue, on=['hour', 'zone_id'], how='outer')
    df_join.coalesce(1).write.parquet(f'{output}/total', mode='overwrite')

    # Load lookup into Spark.n
    df_zones = spark.read \
        .option("header", "true") \
        .csv(f'{lookup}/taxi_zone_lookup.csv')

    # Join zones with yellow & green data.
    df_zones_revenue = df_join.join(df_zones, df_join.zone_id == df_zones.LocationID)

    # Drop ID columns that are not needed in the report.
    df_zones_revenue = df_zones_revenue.drop('LocationID', 'zone_id')

    # Write the end report to file.
    df_zones_revenue.coalesce(1).write.parquet(f'{output}/zones', mode='overwrite')


finally:

    if spark is not None:
        spark.stop()

    elapsed = time.perf_counter() - start_time
    print(f"Total execution time: {elapsed:.2f}s")