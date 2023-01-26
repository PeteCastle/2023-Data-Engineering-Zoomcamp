import pandas as pd
import os
import argparse
from sqlalchemy import create_engine

parser = argparse.ArgumentParser(description="Ingest Parquet data to PostGres")

parser.add_argument('--user', help='postgres username')
parser.add_argument('--password', help='postgres usernapassme')
parser.add_argument('--host', help='postgres username')
parser.add_argument('--port', help='postgres port')
parser.add_argument('--db', help='postgres db')
parser.add_argument('--table_name', help='postgres table name')
parser.add_argument('--url', help='parquet file url')



def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('updating taxi zone data')
    taxi_zone_lookup_url  = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv '
    taxi_zone_lookup_file  = 'taxi+_zone_lookup.csv'
    os.system(f'curl -Ss {taxi_zone_lookup_url} -o datasets/{taxi_zone_lookup_file}')
    taxi_zone_pd = pd.read_csv('datasets/taxi+_zone_lookup.csv')
    taxi_zone_pd.to_sql(name='taxi_zones', con=engine, if_exists='replace')


    print('reading and saving csv file')
    #read csv instead of read_parquet as the parquet file is different from the assignment csv file :(
    #WHY DOES THE FILE IS COMPRESSED!
    #s.system(f'curl -Ss {url} -o datasets/green_taxi_trips.csv')
    df_iterator = pd.read_csv('datasets/green_tripdata_2019-01.csv', parse_dates = True, iterator= True, chunksize = 10000)

    for df in df_iterator:
        df.to_sql(name=f'{table_name}', con=engine, if_exists='append')
        print(df)

    # df = pd.read_parquet(f'{url}')
    # #engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    # schema = pd.io.sql.get_schema(df,name=f'{table_name}')
    # print(schema)

    



if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
