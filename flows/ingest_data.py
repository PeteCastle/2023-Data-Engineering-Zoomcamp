import pandas as pd
import os
import argparse
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

# parser = argparse.ArgumentParser(description="Ingest Parquet data to PostGres")
# parser.add_argument('--user', help='postgres username')
# parser.add_argument('--password', help='postgres usernapassme')
# parser.add_argument('--host', help='postgres username')
# parser.add_argument('--port', help='postgres port')
# parser.add_argument('--db', help='postgres db')
# parser.add_argument('--table_name', help='postgres table name')
# parser.add_argument('--url', help='parquet file url')


@task(log_prints=True, tags=["extract"],cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data( fileName: str, url: str =''):
    if not os.path.exists('../resources/datasets/{fileName}'):
        print(f'File ../datasets/{fileName} does not exist. Downloading from {url}')
        os.system(f'curl -Ss {url} -o resources/datasets/{fileName}')
    return pd.read_csv(f'../resources/datasets/{fileName}', parse_dates = True) #Remove iterator and chunksize for the csv file

@task(log_prints=True, tags=["transform"])
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True, tags=["extract_lookup"],cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data_lookup(taxi_zone_lookup_url: str, taxi_zone_lookup_file: str =''):
    os.system(f'curl -Ss {taxi_zone_lookup_url} -o ../resources/datasets/{taxi_zone_lookup_file}')
    return pd.read_csv(f'../datasets/{taxi_zone_lookup_file}')

@task(log_prints=True, tags=["load"])
def ingest_data(df: pd.DataFrame, table_name: str, mode: str='replace'):

    now = datetime.now()
    connection_block = SqlAlchemyConnector.load("sqlalchemy-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.to_sql(name=f'{table_name}', con=engine, if_exists=mode)
    print(f"Data ingestion done! Took {datetime.now()-now } seconds")
    
@flow(name="Ingest Flow")
def main_flow(params):
    # user = params.user
    # password = params.password
    # host = params.host
    # port = params.port
    # db = params.db
    # table_name = params.table_name
    # url = params.url


    # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # engine.connect()

    # print('updating taxi zone data')
    # taxi_zone_lookup_url  = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv '
    # taxi_zone_lookup_file  = 'taxi+_zone_lookup.csv'
    # os.system(f'curl -Ss {taxi_zone_lookup_url} -o datasets/{taxi_zone_lookup_file}')
    # taxi_zone_pd = pd.read_csv('datasets/taxi+_zone_lookup.csv')

    # taxi_zone_pd.to_sql(name='taxi_zones', con=engine, if_exists='replace')

    csv_file = 'green_tripdata_2019-01.csv'
    table_name = 'green_taxi_trips'
    df = extract_data(csv_file)
    df = transform_data(df)
    ingest_data(df, table_name,'replace')

    #read csv instead of read_parquet as the parquet file is different from the assignment csv file :(
    #WHY DOES THE FILE IS COMPRESSED!
    #os.system(f'curl -Ss {url} -o datasets/green_taxi_trips.csv')
    # df_iterator = pd.read_csv('datasets/green_tripdata_2019-01.csv', parse_dates = True, iterator= True, chunksize = 10000)

    # for df in df_iterator:
    #     df.to_sql(name=f'{table_name}', con=engine, if_exists='append')
    #     print(df)

    # df = pd.read_parquet(f'{url}')
    # #engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    # schema = pd.io.sql.get_schema(df,name=f'{table_name}')
    # print(schema)


if __name__ == "__main__":
    # args = parser.parse_args()
    main_flow({})
