from prefect import flow, task
import pandas as pd
from datetime import timedelta
from pathlib import Path

from parameterized_flow import clean, write_local,write_azure

@task(retries=3, log_prints=True)
def fetch_encoding(dataset_url: str) ->  pd.DataFrame:
    """Fetch data from web"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def write_local_yearly(combined_df : pd.DataFrame, year: int) -> str:
    print(f"Combined df has {len(combined_df)} rows.  Saving the file")

    dataset_file = f"fhv_tripdata_{year}"
    path = Path(f"resources/datasets/{dataset_file}.parquet")

    combined_df.to_parquet(f"resources/datasets/fhv_tripdata_{year}.parquet", index=False, compression="gzip")

    return str(path)

@flow(log_prints=True)
def etl_yearly_flow(  
    years: list[int] = [2020],
    ) -> None:
    """Parent flow to download all datasets within a year, appending to a single parquet file"""
    months = range(1,13)

    for year in years:
        combined_df = pd.DataFrame()
        for month in months:
            print(f"Triggerring yearly flow for fhv {year} {month}")
            dataset_file = f"fhv_tripdata_{year}-{month:02d}"
            dataset_url =  f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

            df :pd.DataFrame = fetch_encoding(dataset_url)
            df = clean(df)
            path = write_local(df,"fhv",dataset_file)

            combined_df = combined_df.append(df, ignore_index=True)

        path = write_local_yearly(combined_df, year)
        write_azure(path) 
        

if __name__ == "__main__":
    etl_yearly_flow()

    