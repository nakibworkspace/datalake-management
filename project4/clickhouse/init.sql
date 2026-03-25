CREATE DATABASE IF NOT EXISTS gold_db;

CREATE TABLE IF NOT EXISTS gold_db.hourly_summary (
    pickup_hour   UInt8,
    trip_count    Int64,
    avg_fare      Float64,
    avg_distance  Float64,
    avg_tip       Float64
) ENGINE = MergeTree()
ORDER BY pickup_hour;

CREATE TABLE IF NOT EXISTS gold_db.location_revenue (
    pu_location_id Int32,
    trip_count     Int64,
    total_revenue  Float64,
    avg_fare       Float64
) ENGINE = MergeTree()
ORDER BY total_revenue;
