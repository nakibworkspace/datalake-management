#!/bin/bash
set -e

echo "=== Lab 04: Medallion Architecture Setup ==="

# Create directory structure
mkdir -p config bronze silver gold clickhouse data lake

# Create __init__.py files for Python packages
touch config/__init__.py bronze/__init__.py silver/__init__.py gold/__init__.py
touch bronze/ingest.py clickhouse/init.sql gold/aggregate.py silver/clean.py
touch docker-compose.yml requirements.txt run_pipeline.py

# Populate config/settings.py
cat > config/settings.py << 'EOF'
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
EOF

echo "Done. Project structure created:"
echo ""
find . -not -path './lake/*' -not -path './.git/*' -not -name '*.parquet' | sort
