#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm


# Specify the data types for each column
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of data to ingest')
@click.option('--month', default=1, type=int, help='Month of data to ingest (1-12)')
@click.option('--table-name', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
@click.option('--url', default=None, help='Custom URL to CSV file (if not specified, uses NYC TLC data URL)')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, table_name, chunksize, url):
    """Ingest NYC taxi data into PostgreSQL database."""
    # Generate URL if not provided
    if url is None:
        prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
        url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'
    
    click.echo(f"Ingesting data from: {url}")
    click.echo(f"Target table: {table_name}")
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

#Ingesting data in chunks since we cannot take the whole dataset at once. We will do batches and use an iterator for that. 
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    first = True

    for df_chunk in tqdm(df_iter):

        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists="replace"
            )
            first = False
            click.echo("Table created successfully")

        # Insert chunk
        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append"
        )

if __name__ == "__main__":
    run()





