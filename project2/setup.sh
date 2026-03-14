#!/bin/bash
# =========================================================
# setup.sh — Scaffold the NYC Taxi Data Warehouse project
# =========================================================
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "Setting up project in: $PROJECT_DIR"

# --- Create directories ---
mkdir -p "$PROJECT_DIR/config"
mkdir -p "$PROJECT_DIR/scripts"
mkdir -p "$PROJECT_DIR/sql/01-schema"
mkdir -p "$PROJECT_DIR/sql/02-seed"
mkdir -p "$PROJECT_DIR/sql/03-aggregates"
mkdir -p "$PROJECT_DIR/sql/04-queries"

# --- Create empty files (touch preserves existing files) ---
touch "$PROJECT_DIR/.env"
touch "$PROJECT_DIR/docker-compose.yml"
touch "$PROJECT_DIR/config/postgresql.conf"
touch "$PROJECT_DIR/sql/01-schema/01-dimensions.sql"
touch "$PROJECT_DIR/sql/01-schema/02-fact-table.sql"
touch "$PROJECT_DIR/sql/02-seed/01-seed-dimensions.sql"
touch "$PROJECT_DIR/sql/02-seed/02-seed-facts.sql"
touch "$PROJECT_DIR/sql/03-aggregates/01-continuous-aggs.sql"
touch "$PROJECT_DIR/sql/04-queries/01-analytics.sql"
touch "$PROJECT_DIR/scripts/init-runner.sh"
touch "$PROJECT_DIR/scripts/verify.sh"
touch "$PROJECT_DIR/scripts/load_data.py"
touch "$PROJECT_DIR/requirements.txt"

# --- Make scripts executable ---
chmod +x "$PROJECT_DIR/scripts/init-runner.sh"
chmod +x "$PROJECT_DIR/scripts/verify.sh"
chmod +x "$PROJECT_DIR/scripts/load_data.py"

echo ""
echo "Project structure created:"
echo ""
find "$PROJECT_DIR" -not -path '*/\.*' | sort | sed "s|$PROJECT_DIR|lab04|;s|[^/]*/|  |g"
echo ""
echo "Done! Next step: fill in the file contents."
