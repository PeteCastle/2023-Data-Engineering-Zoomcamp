import pandas as pd
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

    print("hello are u working")
    #df = pd.read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet')
    df = pd.read_parquet(f'{url}')

    #engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine.connect()

    schema = pd.io.sql.get_schema(df,name=f'{table_name}')
    print(schema)

    df.to_sql(name=f'{table_name}', con=engine, if_exists='replace')

if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
