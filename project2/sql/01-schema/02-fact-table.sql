-- =========================================================
-- Fact Table: fact_trips
-- Range-partitioned by pickup_date (monthly)
-- =========================================================

CREATE TABLE fact_trips (
    trip_id             BIGSERIAL,
    pickup_datetime     TIMESTAMP NOT NULL,
    dropoff_datetime    TIMESTAMP NOT NULL,
    pickup_date         DATE NOT NULL,
    vendor_id           INT REFERENCES dim_vendor(vendor_id),
    passenger_count     SMALLINT,
    trip_distance       NUMERIC(10,2),
    pu_location_id      INT REFERENCES dim_zones(location_id),
    do_location_id      INT REFERENCES dim_zones(location_id),
    rate_code_id        INT REFERENCES dim_rate_code(rate_code_id),
    payment_type        INT REFERENCES dim_payment_type(payment_type_id),
    fare_amount         NUMERIC(10,2),
    tip_amount          NUMERIC(10,2),
    tolls_amount        NUMERIC(10,2),
    total_amount        NUMERIC(10,2),
    PRIMARY KEY (trip_id, pickup_date)
) PARTITION BY RANGE (pickup_date);

-- Monthly partitions: Jan + Feb 2024
CREATE TABLE fact_trips_2024_01 PARTITION OF fact_trips
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE fact_trips_2024_02 PARTITION OF fact_trips
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- BRIN index on pickup_date (tiny index, perfect for time-ordered data)
CREATE INDEX idx_fact_trips_date_brin ON fact_trips USING BRIN (pickup_date)
    WITH (pages_per_range = 32);
