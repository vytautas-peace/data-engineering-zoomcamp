#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of the data to ingest')
@click.option('--month', default=11, type=int, help='Month of the data to ingest')


def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month):

    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df = pd.read_parquet(f'green_tripdata_{year}-{month:02d}.parquet', engine='pyarrow')
    df.to_sql(
    name="green_trip_data",
    con=engine,
    if_exists="replace"
    )
    print("Table created")

    df = pd.read_csv('taxi_zone_lookup.csv')
    df.to_sql(
        name="taxi_zone_lookup",
        con=engine,
        if_exists="replace"
    )
    print("Table created")

if __name__ == "__main__":
    run()
