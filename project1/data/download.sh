#!/usr/bin/env bash
# Download NYC Yellow Taxi trip data (Jan 2024) and taxi zone lookup
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="${SCRIPT_DIR}"

TAXI_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
ZONES_URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

echo "==> Downloading taxi trip data (Parquet)..."
curl -L -o "${DATA_DIR}/yellow_tripdata.parquet" "${TAXI_URL}"

echo "==> Converting Parquet to CSV..."
python3 -c "
import pandas as pd
df = pd.read_parquet('${DATA_DIR}/yellow_tripdata.parquet')
df.to_csv('${DATA_DIR}/yellow_tripdata.csv', index=False)
print(f'Converted {len(df):,} rows to CSV')
"

echo "==> Downloading taxi zone lookup..."
curl -L -o "${DATA_DIR}/taxi_zone_lookup.csv" "${ZONES_URL}"

echo "==> Done. Files in ${DATA_DIR}:"
ls -lh "${DATA_DIR}"/*.csv
