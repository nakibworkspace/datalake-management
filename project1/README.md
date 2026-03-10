# NYC Taxi Trip Analytics — Modern Data Warehouse

An end-to-end data warehouse lab: ingest NYC taxi data into ClickHouse, transform with dbt (Bronze/Silver/Gold), visualize in Grafana, and monitor with Prometheus.

## Architecture

```
NYC Taxi CSV ─→ Python ingest ─→ ClickHouse (raw) ─→ dbt ─→ Grafana
                                        ↑
                                  Prometheus ─→ Grafana (infra panel)
```

**Medallion layers (dbt):**
- **Bronze** — `stg_taxi_trips` (staged, cleaned column names)
- **Silver** — `int_taxi_trips_enriched` (joined with zones, calculated duration, filtered)
- **Gold** — `mart_borough_metrics`, `mart_hourly_demand`, `mart_payment_summary`

## Prerequisites

- Docker & Docker Compose
- Python 3.10+
- pip
- curl
- ~2 GB free disk space (for dataset + containers)

## Quick Start — Step by Step

### Step 1: Start the infrastructure

```bash
docker compose up -d
```

Wait for ClickHouse to become healthy:

```bash
docker compose ps
```

All 4 services (clickhouse, clickhouse-exporter, grafana, prometheus) should show as `running` / `healthy`.

### Step 2: Download the dataset

```bash
bash data/download.sh
```

Downloads NYC Yellow Taxi trip data (Jan 2024, ~3M rows) and converts it to CSV.

### Step 3: Install Python dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Ingest data into ClickHouse

```bash
python scripts/ingest.py
```

Loads the CSV into ClickHouse `raw.taxi_trips` and `raw.taxi_zones` tables. You'll see row counts printed as it progresses.

### Step 5: Run the dbt pipeline

```bash
cd dbt_project
dbt run --profiles-dir .
dbt test --profiles-dir .
cd ..
```

- `dbt run` — builds all models (staging → intermediate → marts)
- `dbt test` — validates data quality (not_null, unique, accepted_values)

### Step 6: Open the dashboards

| Service       | URL                          | Credentials |
|---------------|------------------------------|-------------|
| Grafana       | http://localhost:3001        | admin/admin |
| Prometheus    | http://localhost:9090        | —           |
| ClickHouse UI | http://localhost:8123/play   | —           |

The Grafana dashboard **NYC Taxi Analytics** is provisioned automatically. Navigate to Dashboards in the left sidebar to find it.

---

## Explore: ClickHouse Play UI

Open http://localhost:8123/play and try these queries:

### Basic checks

```sql
-- Total rows ingested
SELECT count() FROM raw.taxi_trips;

-- Zone lookup
SELECT * FROM raw.taxi_zones LIMIT 10;

-- Check dbt models exist
SHOW TABLES FROM analytics;
```

### Gold layer queries

```sql
-- Borough performance: avg tip, fare, trip count
SELECT * FROM analytics.mart_borough_metrics ORDER BY total_trips DESC;

-- Which hour of the day is busiest?
SELECT * FROM analytics.mart_hourly_demand ORDER BY total_trips DESC LIMIT 5;

-- Cash vs Credit Card breakdown
SELECT * FROM analytics.mart_payment_summary;
```

### Ad-hoc analytical queries

```sql
-- Top 10 busiest pickup zones
SELECT
    z.Zone,
    z.Borough,
    count() as trips
FROM raw.taxi_trips t
JOIN raw.taxi_zones z ON t.PULocationID = z.LocationID
GROUP BY z.Zone, z.Borough
ORDER BY trips DESC
LIMIT 10;

-- Average tip percentage by borough
SELECT
    z.Borough,
    round(avg(t.tip_amount / t.fare_amount) * 100, 2) as avg_tip_pct
FROM raw.taxi_trips t
JOIN raw.taxi_zones z ON t.PULocationID = z.LocationID
WHERE t.fare_amount > 0
GROUP BY z.Borough
ORDER BY avg_tip_pct DESC;

-- Trip volume by day of week (1=Monday, 7=Sunday)
SELECT
    toDayOfWeek(tpep_pickup_datetime) as day_of_week,
    count() as trips,
    round(avg(total_amount), 2) as avg_total
FROM raw.taxi_trips
GROUP BY day_of_week
ORDER BY day_of_week;

-- Late night vs daytime average fares
SELECT
    if(toHour(tpep_pickup_datetime) BETWEEN 6 AND 18, 'Daytime (6am-6pm)', 'Night (6pm-6am)') as period,
    count() as trips,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(tip_amount), 2) as avg_tip
FROM raw.taxi_trips
WHERE fare_amount > 0
GROUP BY period;

-- Airport trips: JFK vs LaGuardia
SELECT
    z.Zone as airport,
    count() as trips,
    round(avg(t.fare_amount), 2) as avg_fare,
    round(avg(t.trip_distance), 2) as avg_distance
FROM raw.taxi_trips t
JOIN raw.taxi_zones z ON t.DOLocationID = z.LocationID
WHERE z.Zone IN ('JFK Airport', 'LaGuardia Airport')
GROUP BY z.Zone;
```

---

## Explore: Prometheus UI

Open http://localhost:9090 and try these queries in the **Graph** tab:

### ClickHouse health metrics

```promql
# Total queries executed
clickhouse_queries

# Current number of active queries
clickhouse_queries_running

# Total rows read
clickhouse_read_rows

# Total rows written
clickhouse_written_rows

# Memory usage
clickhouse_memory_usage

# Number of active connections
clickhouse_connections
```

### Prometheus self-monitoring

```promql
# Prometheus scrape duration
scrape_duration_seconds{job="clickhouse"}

# Is the ClickHouse exporter target reachable? (1 = yes)
up{job="clickhouse"}

# Total samples scraped per scrape
scrape_samples_scraped{job="clickhouse"}
```

Go to http://localhost:9090/targets to verify the **clickhouse** target shows status **UP**.

---

## Verify It Worked

```bash
# Check row count in ClickHouse
docker exec clickhouse clickhouse-client --query "SELECT count() FROM raw.taxi_trips"

# Check dbt Gold tables
docker exec clickhouse clickhouse-client --query "SELECT * FROM analytics.mart_borough_metrics"

# Check Prometheus targets
# Open http://localhost:9090/targets — clickhouse exporter should show as UP
```

## Other Useful Commands

```bash
# View container status
docker compose ps

# View ClickHouse logs
docker logs clickhouse

# Generate and serve dbt docs
cd dbt_project && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir . --port 8080

# Stop all containers
docker compose down

# Stop and delete all data (full reset)
docker compose down -v
```

## Project Structure

```
lab01/
├── docker-compose.yml          # ClickHouse, Grafana, Prometheus, Exporter
├── Makefile                    # Optional: shortcut commands
├── requirements.txt            # Python + dbt dependencies
├── data/
│   └── download.sh             # Dataset download script
├── scripts/
│   └── ingest.py               # CSV → ClickHouse loader
├── clickhouse/
│   └── init.sql                # Raw table DDL
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/            # Bronze layer
│   │   ├── intermediate/       # Silver layer
│   │   └── marts/              # Gold layer
│   └── seeds/
│       └── taxi_zone_lookup.csv
├── grafana/
│   ├── provisioning/           # Auto-config datasources + dashboards
│   └── dashboards/             # Pre-built dashboard JSON
└── prometheus/
    └── prometheus.yml
```

## Troubleshooting

**ClickHouse not starting:** Check port 8123/9000 are free. Run `docker logs clickhouse`.

**dbt can't connect:** Ensure ClickHouse is healthy (`docker compose ps`). If running dbt from inside a container, set `CLICKHOUSE_HOST=clickhouse`.

**Grafana shows no data:** Ensure dbt models built successfully. Check that the ClickHouse datasource is configured (Settings → Data Sources in Grafana). You may need to manually set the datasource UID in each panel.

**Grafana dashboard not loading:** Navigate to Dashboards in the left sidebar instead of relying on the home page. The dashboard is named "NYC Taxi Analytics".

**Download fails:** The NYC TLC data URL may change. Check https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page for current links.
