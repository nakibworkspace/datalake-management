from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# Lake layer paths
BRONZE_DIR = BASE_DIR / "lake" / "bronze"
SILVER_DIR = BASE_DIR / "lake" / "silver"
GOLD_DIR = BASE_DIR / "lake" / "gold"

# Source data
RAW_DATA = BASE_DIR / "data" / "yellow_tripdata.parquet"

# ClickHouse
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DB = "gold_db"
