from pathlib import Path
from prefect import flow,task

from prefect_azure import AzureBlobStorageCredentials

from prefect_azure.blob_storage import blob_storage_download
from prefect_sqlalchemy import SqlAlchemyConnector

import pandas as pd


# Made to Synapse (BigQuery equivalent0 as a parameter, similar to flow)
@flow(log_prints=True, name="ETL Parent Flow")
def synapse_parent_flow(
    months: list[int] = [2,3],
    years: list[int] = [2019],
    colors: list[str] = ["yellow"]
):
    """Parent flow to run multiple base flows"""
    for color in colors:
        for year in years:
            for month in months:
                azblob_to_synapse(color, year, month)


@flow(log_prints=True)
def azblob_to_synapse(color: str, year: int, month: int):
    # Main ETL flow to load data into Synapse Analytics

    path = extract_from_azblob(color, year, month)
    df = tansform(path)
    write_cosmos(df, color, year, month)

@task()
def write_cosmos(df: pd.DataFrame, color: str, year: int, month:int) -> None:
    "Write table to PostgreSQL cosmos DB"
    cosmos_block = SqlAlchemyConnector.load("prefect-cosmosdb-sql")
    cosmos_block_engine = cosmos_block.get_engine()

    # df.to_sql(f"{color}_tripdata_{year}-{month:02d}", 
    #     cosmos_block_engine, 
    #     if_exists="replace", 
    #     index=False,
    #     chunksize=100000)

    # Temporarily modified for assignment #2 question 3 completion
    df.to_sql(f"combined_dataframe", 
        cosmos_block_engine, 
        if_exists="append", 
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