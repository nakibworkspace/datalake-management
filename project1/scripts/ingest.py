"""Ingest NYC Taxi CSV data into ClickHouse raw tables."""

import os
import sys
import clickhouse_connect
import pandas as pd

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
TRIPS_CSV = os.path.join(DATA_DIR, "yellow_tripdata.csv")
ZONES_CSV = os.path.join(DATA_DIR, "taxi_zone_lookup.csv")

BATCH_SIZE = 100_000


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
    )


def ingest_zones(client):
    print("==> Ingesting taxi zones...")
    df = pd.read_csv(ZONES_CSV)
    df.columns = ["LocationID", "Borough", "Zone", "service_zone"]
    df["Borough"] = df["Borough"].fillna("Unknown").astype(str)
    df["Zone"] = df["Zone"].fillna("Unknown").astype(str)
    df["service_zone"] = df["service_zone"].fillna("Unknown").astype(str)
    df["LocationID"] = df["LocationID"].fillna(0).astype(int)
    client.insert_df("raw.taxi_zones", df)
    count = client.query("SELECT count() FROM raw.taxi_zones").result_rows[0][0]
    print(f"    Loaded {count:,} zones")


def ingest_trips(client):
    print("==> Ingesting taxi trips (this may take a few minutes)...")
    total = 0
    for chunk in pd.read_csv(TRIPS_CSV, chunksize=BATCH_SIZE, low_memory=False):
        # Parse datetime columns
        chunk["tpep_pickup_datetime"] = pd.to_datetime(chunk["tpep_pickup_datetime"])
        chunk["tpep_dropoff_datetime"] = pd.to_datetime(chunk["tpep_dropoff_datetime"])

        # Fill NaN with defaults
        float_cols = chunk.select_dtypes(include=["float64"]).columns
        chunk[float_cols] = chunk[float_cols].fillna(0.0)
        int_cols = ["VendorID", "PULocationID", "DOLocationID", "payment_type"]
        for col in int_cols:
            chunk[col] = chunk[col].fillna(0).astype(int)
        chunk["store_and_fwd_flag"] = chunk["store_and_fwd_flag"].fillna("N")

        client.insert_df("raw.taxi_trips", chunk)
        total += len(chunk)
        print(f"    Inserted {total:,} rows...")

    count = client.query("SELECT count() FROM raw.taxi_trips").result_rows[0][0]
    print(f"==> Done. Total rows in raw.taxi_trips: {count:,}")


def main():
    if not os.path.exists(TRIPS_CSV):
        print(f"ERROR: {TRIPS_CSV} not found. Run 'make download' first.", file=sys.stderr)
        sys.exit(1)

    client = get_client()
    ingest_zones(client)
    ingest_trips(client)


if __name__ == "__main__":
    main()
