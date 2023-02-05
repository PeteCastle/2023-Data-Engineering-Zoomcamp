from pathlib import Path
from prefect import flow,task

from prefect_azure import AzureBlobStorageCredentials

from prefect_azure.blob_storage import blob_storage_download
from prefect_sqlalchemy import SqlAlchemyConnector

import pandas as pd
@flow()
def azblob_to_synapse():
    # Main ETL flow to load data into Synapse Analytics
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_azblob(color, year, month)
    df = tansform(path)
    write_cosmos(df, color, year, month)

@task()
def write_cosmos(df: pd.DataFrame, color: str, year: int, month:int) -> None:
    "Write table to PostgreSQL cosmos DB"
    cosmos_block = SqlAlchemyConnector.load("prefect-cosmosdb-sql")
    cosmos_block_engine = cosmos_block.get_engine()
    print("GUMANA NA DITO:")

    df.to_sql(f"{color}_tripdata_{year}-{month:02d}", 
        cosmos_block_engine, 
        if_exists="replace", 
        index=False,
        chunksize=100000)
   

# @flow(log_prints=True)
# def extract_from_azblob(color: str, year: int, month: int) -> Path:
#     azblob_path = f"data/{color}_tripdata_{year}-{month:02d}.parquet"
#     azblob_block = AzureBlobStorageCredentials.load("azure-prefect")
#     directory = blob_storage_list(
#         container ="zoomcampcontainer",  
#         blob_storage_credentials=azblob_block)[0]
#     print(type(directory))
#     print(directory)

@flow(log_prints=True)
def extract_from_azblob(color: str, year: int, month: int) -> Path:
    fileName = f"{color}_tripdata_{year}-{month:02d}.parquet"
    filePath = Path("./resources/datasets/"+fileName)
    azblob_block = AzureBlobStorageCredentials.load("azure-prefect")
    file_byte = blob_storage_download(
        blob = fileName,
        container ="zoomcampcontainer",  
        blob_storage_credentials=azblob_block)
    
    if filePath.exists():
        filePath.unlink()


    with open(filePath.absolute(), "wb") as f:
        f.write(file_byte)

    return filePath


@task(log_prints=True)
def tansform(path: Path)-> pd.DataFrame:
    # Data Cleaning
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'] = df['passenger_count'].fillna(0)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

if __name__ == "__main__":
    azblob_to_synapse()