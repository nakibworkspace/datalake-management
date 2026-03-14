#!/bin/bash
# =========================================================
# Verification script for the NYC Taxi Data Warehouse
# Run: bash scripts/verify.sh
# =========================================================
set -e

CONTAINER="taxi_warehouse"

source .env 2>/dev/null || true
DB_USER="${POSTGRES_USER:-warehouse_admin}"
DB_NAME="${POSTGRES_DB:-taxi_warehouse}"

PSQL="docker exec -i $CONTAINER psql -U $DB_USER -d $DB_NAME -t -A"

echo "========================================"
echo "  NYC Taxi Warehouse — Verification"
echo "========================================"

echo ""
echo "[1] TimescaleDB Extension"
$PSQL -c "SELECT extname || ' v' || extversion FROM pg_extension WHERE extname = 'timescaledb';"

echo ""
echo "[2] Dimension Row Counts"
for tbl in dim_date dim_zones dim_payment_type dim_rate_code dim_vendor; do
  count=$($PSQL -c "SELECT COUNT(*) FROM $tbl;")
  printf "  %-20s %s rows\n" "$tbl" "$count"
done

echo ""
echo "[3] Fact Table Row Count"
count=$($PSQL -c "SELECT COUNT(*) FROM fact_trips;")
printf "  %-20s %s rows\n" "fact_trips" "$count"

echo ""
echo "[4] Partition Count"
$PSQL -c "SELECT COUNT(*) || ' partitions' FROM pg_catalog.pg_inherits WHERE inhparent = 'fact_trips'::regclass;"

echo ""
echo "[5] BRIN Index"
$PSQL -c "SELECT indexname FROM pg_indexes WHERE indexname = 'idx_fact_trips_date_brin';"

echo ""
echo "[6] Materialized Views"
$PSQL -c "SELECT matviewname FROM pg_matviews WHERE schemaname = 'public' ORDER BY matviewname;"

echo ""
echo "[7] Sample: Daily Stats (first 5 days)"
$PSQL -c "
SELECT full_date, day_name, total_trips, daily_revenue
FROM mv_daily_stats
ORDER BY full_date
LIMIT 5;
"

echo ""
echo "[8] Sample: Top 5 Pickup Zones"
$PSQL -c "
SELECT z.zone, COUNT(*) AS trips
FROM fact_trips f
JOIN dim_zones z ON f.pu_location_id = z.location_id
GROUP BY z.zone
ORDER BY trips DESC
LIMIT 5;
"

echo ""
echo "[9] Partition Pruning Check (January 2024)"
$PSQL -c "
EXPLAIN (COSTS OFF)
SELECT SUM(total_amount) FROM fact_trips
WHERE pickup_date >= '2024-01-01' AND pickup_date < '2024-02-01';
" | head -6

echo ""
echo "========================================"
echo "  All checks complete!"
echo "========================================"
