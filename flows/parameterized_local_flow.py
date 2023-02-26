from pathlib import Path
import pandas as pd
from prefect import flow, task

# from prefect_gcp.cloud_storage import GcsBucket changed to azure
from datetime import timedelta
from random import randint

from prefect.tasks import task_input_hash

from datetime import timedelta

import os

@task(log_prints=True, tags=["load"])
def download_local(url: str, color: str, year:int, month:int, dataset_file: str) -> str:
    """Write pandas daaframe as a parquet"""
    path = Path(f"{os.getcwd()}/resources/datasets/{color}/{year}/{month:02d}/{dataset_file}.parquet")

    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    os.system('curl -o '+ '\"' + str(path)+'\" '+url)
    print(f"Saved file {path}")
    return str(path)


@flow()
def etl_web_to_local(color: str, year:int, month:int) -> None:
    """Extract, transform, and load data from web to GCS"""
    print(f"Triggerring flow for {color} {year} {month}")
    dataset_file = f"{color}_tripdata_{year}-{month:02d}"
    dataset_url =  f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02d}.parquet"

    # df : pd.DataFrame = pd.read_parquet(dataset_url)

    path = download_local(dataset_url,color,year,month,dataset_file)

# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet

@flow()
def etl_local_flow(  # Note that the list below is just a sample, should ALWAYS pass a parameter on flow-deployment.yaml
    months: list[int] = [6],
    years: list[int] = [2021],
    colors: list[str] = ["fhvhv"]
    ) -> None:
    """Parent flow to run multiple child flows"""
    for color in colors:
        for year in years:
            for month in months:
                etl_web_to_local(color, year, month)

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
    etl_local_flow()