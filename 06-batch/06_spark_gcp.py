#!/usr/bin/env python
# coding: utf-8

import time

# Initiate session.
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


start_time = time.perf_counter()
spark = None

try:

    credentials_location = '/home/vytautas.peace/data-engineering-zoomcamp/.google/credentials/spark_gcp.json'

    conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName('test') \
        .set("spark.jars", "/home/vytautas.peace/data-engineering-zoomcamp/06-batch/lib/gcs-connector-hadoop3-2.2.5.jar") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)


    # Stop the existing context if it exists
    try:
        sc.stop()
    except:
        pass

    # Now initialize your new one with your GCS config
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    hadoop_conf = sc._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()


    # Read green data, normalise column names, register a table for running SQL.
    df_green = spark.read.parquet('gs://nyc-tlc-data-lake/pq/green/*/*')
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
    df_green_revenue.coalesce(1).write.parquet('gs://nyc-tlc-data-lake/report/revenue/green', mode='overwrite')

    # Read yellow data, normalise column names, register a table for running SQL.
    df_yellow = spark.read.parquet('gs://nyc-tlc-data-lake/pq/yellow/*/*')
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
    df_yellow_revenue.coalesce(1).write.parquet('gs://nyc-tlc-data-lake/report/revenue/yellow', mode='overwrite')

    # Outer join & write to file.
    df_join = df_green_revenue.join(df_yellow_revenue, on=['hour', 'zone_id'], how='outer')
    df_join.coalesce(1).write.parquet('gs://nyc-tlc-data-lake/report/revenue/total', mode='overwrite')

    # Load lookup into Spark.
    df_zones = spark.read \
        .option("header", "true") \
        .csv('gs://nyc-tlc-data-lake/csv/taxi_zone_lookup.csv')

    # Join zones with yellow & green data.
    df_zones_revenue = df_join.join(df_zones, df_join.zone_id == df_zones.LocationID)

    # Drop ID columns that are not needed in the report.
    df_zones_revenue = df_zones_revenue.drop('LocationID', 'zone_id')

    # Write the end report to file.
    df_zones_revenue.coalesce(1).write.parquet('gs://nyc-tlc-data-lake/report/revenue/zones', mode='overwrite')


finally:

    if spark is not None:
        spark.stop()

    elapsed = time.perf_counter() - start_time
    print(f"Total execution time: {elapsed:.2f}s")