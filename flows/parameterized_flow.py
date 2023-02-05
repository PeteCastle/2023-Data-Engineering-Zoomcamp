from pathlib import Path
import pandas as pd
from prefect import flow, task

# from prefect_gcp.cloud_storage import GcsBucket changed to azure
from prefect_azure import AzureBlobStorageCredentials
from prefect_azure.blob_storage import blob_storage_upload

from datetime import timedelta
from random import randint

from prefect.tasks import task_input_hash

from datetime import timedelta

@task(retries=3)
def fetch(dataset_url: str) ->  pd.DataFrame:
    """Fetch data from web"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True, tags=["transform"])
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    #this is optional, can simply use parse_Dates=True in fetch()

    if 'tpep_pickup_datetime' in df.columns:
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    elif 'lpep_pickup_datetime' in df.columns:
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    print(df.head(5))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task(log_prints=True, tags=["load"])
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> str:
    """Write pandas daaframe as a parquet"""
    path = Path(f"resources/datasets/{dataset_file}.parquet")
    df.to_parquet(path, index=False, compression="gzip")
    print(f"Saved file {path}")
    return str(path)

@flow(log_prints=True)
def write_azure(path:str) -> None:
    """Upload parquet file to Azure"""

    azure_credentials_block = AzureBlobStorageCredentials.load("azure-prefect")
    print(type(azure_credentials_block))
    fileName = Path(path).name
    with open(path, "rb") as f:
        data = f.read()
        blob_storage_upload(data=data,container="zoomcampcontainer",blob=fileName, blob_storage_credentials=azure_credentials_block,  overwrite=True)
    
    print(f"Uploaded file {path} to Azure storage account")


    # bucket = GcsBucket(bucket_name="data-talks-club")
    
@flow()
def etl_web_to_azure(color: str, year:int, month:int) -> None:
    """Extract, transform, and load data from web to GCS"""
    print(f"Triggerring flow for {color} {year} {month}")
    dataset_file = f"{color}_tripdata_{year}-{month:02d}"
    dataset_url =  f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df : pd.DataFrame = fetch(dataset_url)
    df = clean(df)

    path = write_local(df,color,dataset_file)

    write_azure(path)

@flow()
def etl_parent_flow(  # Note that the list below is just a sample, should ALWAYS pass a parameter on flow-deployment.yaml
    months: list[int] = [1,2,3,4],
    years: list[int] = [2021],
    colors: list[str] = ["yellow"]
    ) -> None:
    """Parent flow to run multiple child flows"""
    for color in colors:
        for year in years:
            for month in months:
                etl_web_to_azure(color, year, month)

# @flow()
# def etl_web_to_azure() -> None:
#     """Extract, transform, and load data from web to GCS"""
#     color = "yellow"
#     year = 2021
#     month = 1
#     dataset_file = f"{color}_tripdata_{year}-{month:02d}"
#     dataset_url =  f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

#     df : pd.DataFrame = fetch(dataset_url)
#     df = clean(df)

#     path = write_local(df,color,dataset_file)

#     write_azure(path)

if __name__ == "__main__":
    etl_parent_flow()