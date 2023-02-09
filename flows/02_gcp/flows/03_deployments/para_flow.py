from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
import os

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url :str) -> pd.DataFrame:
    # getting file and loading into df

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df:pd.DataFrame)->pd.DataFrame:
    # fix data formatting issues.
    df["lpep_pickup_datetime"]= pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"]= pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns:{df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task
def write_to_local(df:pd.DataFrame,color:str,dataset_file:str)-> Path:
    # making directory if it doesn't exist
    directory = f"data/{color}"
    if not os.path.exists(directory):
        os.makedirs(directory)
    path = Path(f"{directory}/{dataset_file}.parquet")

    #saving to parquet
    df.to_parquet(path,compression="gzip")
    return path

@task
def write_to_gcs(path:Path)->None:

    gcp_bucket = GcsBucket.load("zoom-gcs")
    gcp_bucket.upload_from_path(from_path=path,to_path=path)
    return None

@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_to_local(df_clean, color, dataset_file)
    write_to_gcs(path)

@flow()
def etl_parent_flow(months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"):
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == "__main__":
    color = "yellow"
    months = [1, 2, 3]
    year = 2021
    etl_parent_flow(months,year,color)
