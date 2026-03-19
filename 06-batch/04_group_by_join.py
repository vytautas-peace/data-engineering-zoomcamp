#!/usr/bin/env python
# coding: utf-8

import os
import time
import urllib.request

# Initiate session.
from pyspark.sql import SparkSession

start_time = time.perf_counter()
spark = None

try:
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()

    # Run this right after creating your 'spark' session
    spark.sparkContext.setLogLevel("ERROR")

    # Read green data, normalise column names, register a table for running SQL.
    df_green = spark.read.parquet('data/raw/green/*/*')
    df_green = df_green \
        .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
    print(f"df_green records: {df_green.count()}")
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
    df_green_revenue.coalesce(1).write.parquet('data/report/revenue/green', mode='overwrite')

    # Read yellow data, normalise column names, register a table for running SQL.
    df_yellow = spark.read.parquet('data/raw/yellow/*/*')
    df_yellow = df_yellow \
        .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
    print(f"df_yellow records: {df_yellow.count()}")
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
    df_yellow_revenue.coalesce(1).write.parquet('data/report/revenue/yellow', mode='overwrite')

    # Outer join & write to file.
    df_join = df_green_revenue.join(df_yellow_revenue, on=['hour', 'zone_id'], how='outer')
    df_join.coalesce(1).write.parquet('data/report/revenue/total', mode='overwrite')

    # Download lookup file.
    if not os.path.exists('data/raw/lookup'):
        os.makedirs('data/raw/lookup')

    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    file_name = 'data/raw/lookup/' + os.path.basename(url)
    if not os.path.exists(file_name):
        urllib.request.urlretrieve(url, file_name)

    # Load lookup into Spark.
    df_zones = spark.read \
        .option("header", "true") \
        .csv('data/raw/lookup/taxi_zone_lookup.csv')

    # Join zones with yellow & green data.
    df_zones_revenue = df_join.join(df_zones, df_join.zone_id == df_zones.LocationID)

    # Drop ID columns that are not needed in the report.
    df_zones_revenue = df_zones_revenue.drop('LocationID', 'zone_id')

    # Write the end report to file.
    df_zones_revenue.coalesce(1).write.parquet('data/report/revenue/zones', mode='overwrite')


finally:

    if spark is not None:
        spark.stop()

    elapsed = time.perf_counter() - start_time
    print(f"Total execution time: {elapsed:.2f}s")

