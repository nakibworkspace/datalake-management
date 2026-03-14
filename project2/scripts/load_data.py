#!/usr/bin/env python3
"""
NYC Yellow Taxi Data Loader
Downloads parquet files and bulk-loads into the warehouse.
"""

import os
import io
import time
import psycopg2
import pandas as pd
import urllib.request

# --- Config ---
DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
DB_PORT = os.environ.get("POSTGRES_PORT_INTERNAL", os.environ.get("POSTGRES_PORT", "5433"))
DB_USER = os.environ.get("POSTGRES_USER", "warehouse_admin")
DB_NAME = os.environ.get("POSTGRES_DB", "taxi_warehouse")
DB_PASS = os.environ.get("POSTGRES_PASSWORD", "warehouse_pass_2026")

PARQUET_URLS = [
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
]

ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

KEEP_COLS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "VendorID", "passenger_count", "trip_distance",
    "PULocationID", "DOLocationID", "RatecodeID",
    "payment_type", "fare_amount", "tip_amount",
    "tolls_amount", "total_amount",
]


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, dbname=DB_NAME, password=DB_PASS)


def wait_for_db():
    """Wait until Postgres is ready."""
    print("Waiting for database...")
    for _ in range(60):
        try:
            conn = get_conn()
            conn.close()
            print("Database is ready!")
            return
        except psycopg2.OperationalError:
            time.sleep(2)
    raise Exception("Database not ready after 120s")


def load_zones():
    """Download and load taxi zone lookup CSV."""
    print("Downloading taxi zone lookup...")
    response = urllib.request.urlopen(ZONES_URL)
    df = pd.read_csv(io.BytesIO(response.read()))
    df.columns = df.columns.str.strip()

    conn = get_conn()
    cur = conn.cursor()
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur.copy_from(buf, "dim_zones", sep=",", columns=["location_id", "borough", "zone", "service_zone"])
    conn.commit()
    cur.close()
    conn.close()
    print(f"  Loaded {len(df)} zones.")


def load_trips(url):
    """Download a parquet file and load trips into fact_trips."""
    filename = url.split("/")[-1]
    print(f"Downloading {filename}...")

    response = urllib.request.urlopen(url)
    df = pd.read_parquet(io.BytesIO(response.read()), columns=KEEP_COLS)
    print(f"  Downloaded {len(df):,} rows")

    df = df.rename(columns={
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "VendorID": "vendor_id",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "RatecodeID": "rate_code_id",
    })

    df["pickup_date"] = pd.to_datetime(df["pickup_datetime"]).dt.date

    # Filter to only Jan/Feb 2024 (some rows have bad dates)
    df = df[
        (df["pickup_date"] >= pd.Timestamp("2024-01-01").date()) &
        (df["pickup_date"] < pd.Timestamp("2024-03-01").date())
    ]

    # Handle nulls
    df["rate_code_id"] = df["rate_code_id"].fillna(99).astype(int)
    df["vendor_id"] = df["vendor_id"].fillna(1).astype(int)
    df["payment_type"] = df["payment_type"].fillna(5).astype(int)

    # Clamp to known values
    df.loc[~df["rate_code_id"].isin([1, 2, 3, 4, 5, 6, 99]), "rate_code_id"] = 99
    df.loc[~df["vendor_id"].isin([1, 2]), "vendor_id"] = 1
    df.loc[~df["payment_type"].isin([1, 2, 3, 4, 5, 6]), "payment_type"] = 5

    out_cols = [
        "pickup_datetime", "dropoff_datetime", "pickup_date",
        "vendor_id", "passenger_count", "trip_distance",
        "pu_location_id", "do_location_id", "rate_code_id",
        "payment_type", "fare_amount", "tip_amount",
        "tolls_amount", "total_amount",
    ]

    print(f"  Loading {len(df):,} trips into fact_trips...")
    conn = get_conn()
    cur = conn.cursor()
    buf = io.StringIO()
    df[out_cols].to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur.copy_from(buf, "fact_trips", sep=",", null="", columns=out_cols)
    conn.commit()
    cur.close()
    conn.close()
    print(f"  Done loading {filename}.")


def build_aggregates():
    """Analyze tables and refresh materialized views."""
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    print("Running ANALYZE on fact_trips...")
    cur.execute("ANALYZE fact_trips;")

    print("Refreshing materialized views...")
    for mv in ["mv_daily_stats", "mv_hourly_demand", "mv_revenue_by_payment"]:
        print(f"  Refreshing {mv}...")
        cur.execute(f"REFRESH MATERIALIZED VIEW {mv};")

    cur.close()
    conn.close()
    print("Aggregates refreshed.")


def main():
    wait_for_db()

    # Check if data already loaded
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM fact_trips;")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    if count > 100000:
        print(f"fact_trips already has {count:,} rows. Skipping load.")
        return

    print("=" * 50)
    print("  NYC Yellow Taxi Data Loader")
    print("=" * 50)

    load_zones()

    for url in PARQUET_URLS:
        load_trips(url)

    build_aggregates()

    print("=" * 50)
    print("  Loading complete!")
    print("=" * 50)


if __name__ == "__main__":
    main()
