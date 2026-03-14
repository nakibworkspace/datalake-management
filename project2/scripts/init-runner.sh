#!/bin/bash
# Runs all SQL files in order during first container startup.
set -e

echo "=== Taxi Warehouse: Running init scripts ==="

for dir in 01-schema 02-seed 03-aggregates; do
  for f in /docker-entrypoint-initdb.d/sql/${dir}/*.sql; do
    if [ -f "$f" ]; then
      echo ">>> Running: $f"
      psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$f"
    fi
  done
done

echo "=== Taxi Warehouse: Init complete ==="
