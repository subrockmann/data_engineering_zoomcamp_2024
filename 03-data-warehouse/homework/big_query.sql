-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-413213.ny_taxi_dataset.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://subrockmann-zoomcamp-data-bucket/green/green_tripdata_2022-*.parquet']
);

-- Creating table in Big Query 
CREATE OR REPLACE TABLE `de-zoomcamp-413213.ny_taxi_dataset.nonpartitioned_green_tripdata`
AS SELECT * FROM `de-zoomcamp-413213.ny_taxi_dataset.external_green_tripdata`;

-- Question 1
SELECT count(*) as trips
FROM de-zoomcamp-413213.ny_taxi_dataset.external_green_tripdata;


-- Question 2 - external -> Result 0
SELECT COUNT(DISTINCT(PULocationID)) FROM `de-zoomcamp-413213.ny_taxi_dataset.external_green_tripdata`;

-- Question 2 - internal -> Result 6.41 MB
SELECT COUNT(DISTINCT(PULocationID)) FROM `de-zoomcamp-413213.ny_taxi_dataset.nonpartitioned_green_tripdata`;

-- Question 3 -> 1622
SELECT COUNT(*) FROM `de-zoomcamp-413213.ny_taxi_dataset.external_green_tripdata`
WHERE fare_amount is 0;

-- Question 4 
CREATE OR REPLACE TABLE `de-zoomcamp-413213.ny_taxi_dataset.partitioned_green_tripdata`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS (
  SELECT * FROM `de-zoomcamp-413213.ny_taxi_dataset.nonpartitioned_green_tripdata`
);

-- Question 5 - partitioned -> Result 1.12 MB
SELECT COUNT(DISTINCT(PULocationID)) FROM `de-zoomcamp-413213.ny_taxi_dataset.partitioned_green_tripdata`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Question 5 - internal -> Result 12.82 MB
SELECT COUNT(DISTINCT(PULocationID)) FROM `de-zoomcamp-413213.ny_taxi_dataset.nonpartitioned_green_tripdata`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Question 6 - GCP bucket

-- Question 7 - False