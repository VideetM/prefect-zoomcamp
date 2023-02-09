-- Q1
CREATE
OR REPLACE EXTERNAL TABLE `dtc-de-375706.ny_taxi_flv.external_fhv_tripdata` OPTIONS (
    format = 'CSV',
    uris = ['gs://prefect_de_camp/data/fhv/fhv_tripdata_2019-*.csv']
);

CREATE
OR REPLACE TABLE `dtc-de-375706.ny_taxi_flv.fhv_tripdata_non_partitioned` AS
SELECT
    *
FROM
    `dtc-de-375706.ny_taxi_flv.external_fhv_tripdata` 

-- Q2
select
    count(distinct Affiliated_base_number)
from
    `dtc-de-375706.ny_taxi_flv.external_fhv_tripdata` -- `dtc-de-375706.ny_taxi_flv.fhv_tripdata_non_partitioned` AS
select
    count(distinct Affiliated_base_number)
from
    `dtc-de-375706.ny_taxi_flv.fhv_tripdata_non_partitioned` 
-- Q3
select
    count(*)
from
    `dtc-de-375706.ny_taxi_flv.fhv_tripdata_non_partitioned`
where
    PUlocationID is null
    and DOlocationID is null 

-- Q4
    CREATE
    OR REPLACE TABLE `dtc-de-375706.ny_taxi_flv.fhv_tripdata_partition_clustered` PARTITION BY DATE(pickup_datetime) CLUSTER BY affiliated_base_number AS
SELECT
    *
FROM
    `dtc-de-375706.ny_taxi_flv.fhv_tripdata_non_partitioned` 
--Q5
select
    distinct Affiliated_base_number
from
    dtc - de -375706.ny_taxi_flv.fhv_tripdata_non_partitioned
where
    DATE(pickup_datetime) BETWEEN '2019-03-01'
    AND '2019-03-31'
select
    distinct Affiliated_base_number
from
    dtc - de -375706.ny_taxi_flv.fhv_tripdata_partition_clustered
where
    DATE(pickup_datetime) BETWEEN '2019-03-01'
    AND '2019-03-31' 
    
--Q6 
#GCP Bucket
-- Q7
# False

-- Q8
-- Refer to file flv_parquet_web_to_gcs.py