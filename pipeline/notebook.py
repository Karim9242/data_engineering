#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# 1. Connection Parameters
pg_user = 'root'
pg_password = 'root'
pg_host = 'localhost'
pg_port = 5432
pg_db = 'ny_taxi'

# 2. URL Construction 
# Fixed: month:02d ensures it looks for '01' instead of '1' in the GitHub URL
year = 2021
month = 1
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

# 3. Schema Definitions
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

parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

# 4. Create Engine
engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

# 5. Initialize the Iterator
# Removed the redundant pd.read_csv calls to save RAM
df_iter = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000,
)

# 6. Integrated Ingestion Logic (from your image)
first = True
target_table = 'yellow_taxi_data'



for df_chunk in tqdm(df_iter):
    if first:
        # Create the table schema (replace if it already exists)
        # .head(0) takes only the column names, no data
        df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists='replace')
        first = False
    
    # Append the actual data rows from the current chunk
    df_chunk.to_sql(name=target_table, con=engine, if_exists='append')

print("Finished ingesting data into the Postgres database.")