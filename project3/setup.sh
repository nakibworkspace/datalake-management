#!/bin/bash
# =========================================================
# setup.sh — Scaffold the Lab 02 Data Lake project
# =========================================================
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "Setting up project in: $PROJECT_DIR"

# --- Create directories ---
mkdir -p "$PROJECT_DIR/scripts"
mkdir -p "$PROJECT_DIR/spark"

# --- Create empty files ---
touch "$PROJECT_DIR/.env.example"
touch "$PROJECT_DIR/docker-compose.yml"
touch "$PROJECT_DIR/requirements.txt"
touch "$PROJECT_DIR/scripts/ingest.py"
touch "$PROJECT_DIR/scripts/process.py"
touch "$PROJECT_DIR/scripts/query.py"
touch "$PROJECT_DIR/spark/Dockerfile"
touch "$PROJECT_DIR/spark/requirements.txt"

echo ""
echo "Project structure created:"
echo ""
find "$PROJECT_DIR" -not -path '*/\.*' -not -path '*/architecture*' | sort | sed "s|$PROJECT_DIR|lab02|;s|[^/]*/|  |g"
echo ""
echo "Done! Next step: fill in the file contents."
