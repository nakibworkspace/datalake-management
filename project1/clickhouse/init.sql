CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.taxi_trips
(
    VendorID              Int32,
    tpep_pickup_datetime  DateTime,
    tpep_dropoff_datetime DateTime,
    passenger_count       Float32,
    trip_distance         Float64,
    RatecodeID            Float32,
    store_and_fwd_flag    String,
    PULocationID          Int32,
    DOLocationID          Int32,
    payment_type          Int32,
    fare_amount           Float64,
    extra                 Float64,
    mta_tax               Float64,
    tip_amount            Float64,
    tolls_amount          Float64,
    improvement_surcharge Float64,
    total_amount          Float64,
    congestion_surcharge  Float64,
    Airport_fee           Float64
)
ENGINE = MergeTree()
ORDER BY tpep_pickup_datetime;

CREATE TABLE IF NOT EXISTS raw.taxi_zones
(
    LocationID  Int32,
    Borough     String,
    Zone        String,
    service_zone String
)
ENGINE = MergeTree()
ORDER BY LocationID;
