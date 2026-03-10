#!/usr/bin/env bash
# Setup script: creates all directories and files BEFORE docker compose up
# Run this once on a fresh VM to avoid Docker creating ghost directories
set -euo pipefail

echo "==> Creating directory structure..."
mkdir -p data scripts clickhouse
mkdir -p dbt_project/{models/{staging,intermediate,marts},seeds}
mkdir -p grafana/provisioning/{datasources,dashboards}
mkdir -p grafana/dashboards
mkdir -p prometheus

# ─── clickhouse/init.sql ───────────────────────────────────────
echo "==> Writing clickhouse/init.sql"
cat > clickhouse/init.sql << 'SQLEOF'
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.taxi_trips
(
    VendorID              Int32,
    tpep_pickup_datetime  DateTime,
    tpep_dropoff_datetime DateTime,
    passenger_count       Float32,
    trip_distance         Float64,
    RatecodeID            Float32,
    store_and_fwd_flag    String,
    PULocationID          Int32,
    DOLocationID          Int32,
    payment_type          Int32,
    fare_amount           Float64,
    extra                 Float64,
    mta_tax               Float64,
    tip_amount            Float64,
    tolls_amount          Float64,
    improvement_surcharge Float64,
    total_amount          Float64,
    congestion_surcharge  Float64,
    Airport_fee           Float64
)
ENGINE = MergeTree()
ORDER BY tpep_pickup_datetime;

CREATE TABLE IF NOT EXISTS raw.taxi_zones
(
    LocationID  Int32,
    Borough     String,
    Zone        String,
    service_zone String
)
ENGINE = MergeTree()
ORDER BY LocationID;
SQLEOF

# ─── clickhouse/prometheus.xml ────────────────────────────────
echo "==> Writing clickhouse/prometheus.xml"
cat > clickhouse/prometheus.xml << 'EOF'
<clickhouse>
    <http_handlers>
        <defaults/>
        <rule>
            <url>/metrics</url>
            <methods>GET</methods>
            <handler>
                <type>prometheus</type>
                <metrics>true</metrics>
                <events>true</events>
                <asynchronous_metrics>true</asynchronous_metrics>
            </handler>
        </rule>
    </http_handlers>
</clickhouse>
EOF

# ─── prometheus/prometheus.yml ─────────────────────────────────
echo "==> Writing prometheus/prometheus.yml"
cat > prometheus/prometheus.yml << 'EOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: "clickhouse"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["clickhouse:8123"]

  - job_name: "prometheus"
    static_configs:
      - targets: ["localhost:9090"]
EOF

# ─── grafana/provisioning/datasources/clickhouse.yml ───────────
echo "==> Writing grafana/provisioning/datasources/clickhouse.yml"
cat > grafana/provisioning/datasources/clickhouse.yml << 'EOF'
apiVersion: 1

datasources:
  - name: ClickHouse
    type: grafana-clickhouse-datasource
    access: proxy
    isDefault: true
    jsonData:
      host: clickhouse
      port: 9000
      protocol: native
      defaultDatabase: analytics
      username: default
    secureJsonData:
      password: ""

  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: false
EOF

# ─── grafana/provisioning/dashboards/dashboards.yml ────────────
echo "==> Writing grafana/provisioning/dashboards/dashboards.yml"
cat > grafana/provisioning/dashboards/dashboards.yml << 'EOF'
apiVersion: 1

providers:
  - name: default
    orgId: 1
    folder: ""
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: /var/lib/grafana/dashboards
      foldersFromFilesStructure: false
EOF

# ─── grafana/dashboards/nyc_taxi.json ──────────────────────────
echo "==> Writing grafana/dashboards/nyc_taxi.json"
cat > grafana/dashboards/nyc_taxi.json << 'JSONEOF'
{
  "annotations": { "list": [] },
  "editable": true,
  "fiscalYearStartMonth": 0,
  "graphTooltip": 1,
  "id": null,
  "links": [],
  "panels": [
    {
      "gridPos": { "h": 4, "w": 6, "x": 0, "y": 0 },
      "id": 1,
      "title": "Total Trips",
      "type": "stat",
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "" },
      "fieldConfig": {
        "defaults": {
          "thresholds": { "mode": "absolute", "steps": [{ "color": "blue", "value": null }] },
          "unit": "short"
        },
        "overrides": []
      },
      "options": { "colorMode": "value", "graphMode": "none", "justifyMode": "auto", "textMode": "auto", "reduceOptions": { "calcs": ["lastNotNull"] } },
      "targets": [{ "queryType": "sql", "rawSql": "SELECT sum(total_trips) as total_trips FROM analytics.mart_borough_metrics", "refId": "A" }]
    },
    {
      "gridPos": { "h": 4, "w": 6, "x": 6, "y": 0 },
      "id": 2,
      "title": "Avg Fare",
      "type": "stat",
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "" },
      "fieldConfig": {
        "defaults": {
          "thresholds": { "mode": "absolute", "steps": [{ "color": "green", "value": null }] },
          "unit": "currencyUSD"
        },
        "overrides": []
      },
      "options": { "colorMode": "value", "graphMode": "none", "justifyMode": "auto", "textMode": "auto", "reduceOptions": { "calcs": ["lastNotNull"] } },
      "targets": [{ "queryType": "sql", "rawSql": "SELECT round(sum(avg_fare * total_trips) / sum(total_trips), 2) as avg_fare FROM analytics.mart_borough_metrics", "refId": "A" }]
    },
    {
      "gridPos": { "h": 4, "w": 6, "x": 12, "y": 0 },
      "id": 3,
      "title": "Avg Tip",
      "type": "stat",
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "" },
      "fieldConfig": {
        "defaults": {
          "thresholds": { "mode": "absolute", "steps": [{ "color": "orange", "value": null }] },
          "unit": "currencyUSD"
        },
        "overrides": []
      },
      "options": { "colorMode": "value", "graphMode": "none", "justifyMode": "auto", "textMode": "auto", "reduceOptions": { "calcs": ["lastNotNull"] } },
      "targets": [{ "queryType": "sql", "rawSql": "SELECT round(sum(avg_tip * total_trips) / sum(total_trips), 2) as avg_tip FROM analytics.mart_borough_metrics", "refId": "A" }]
    },
    {
      "gridPos": { "h": 4, "w": 6, "x": 18, "y": 0 },
      "id": 4,
      "title": "Total Revenue",
      "type": "stat",
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "" },
      "fieldConfig": {
        "defaults": {
          "thresholds": { "mode": "absolute", "steps": [{ "color": "purple", "value": null }] },
          "unit": "currencyUSD"
        },
        "overrides": []
      },
      "options": { "colorMode": "value", "graphMode": "none", "justifyMode": "auto", "textMode": "auto", "reduceOptions": { "calcs": ["lastNotNull"] } },
      "targets": [{ "queryType": "sql", "rawSql": "SELECT sum(total_revenue) as total_revenue FROM analytics.mart_borough_metrics", "refId": "A" }]
    },
    {
      "gridPos": { "h": 10, "w": 12, "x": 0, "y": 4 },
      "id": 5,
      "title": "Trips & Avg Tip by Borough",
      "type": "barchart",
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "" },
      "fieldConfig": {
        "defaults": { "unit": "short" },
        "overrides": [{ "matcher": { "id": "byName", "options": "avg_tip" }, "properties": [{ "id": "unit", "value": "currencyUSD" }, { "id": "custom.axisPlacement", "value": "right" }] }]
      },
      "options": { "orientation": "horizontal", "showValue": "auto", "barWidth": 0.8 },
      "targets": [{ "queryType": "sql", "rawSql": "SELECT pickup_borough, total_trips, avg_tip FROM analytics.mart_borough_metrics ORDER BY total_trips DESC", "refId": "A" }]
    },
    {
      "gridPos": { "h": 10, "w": 12, "x": 12, "y": 4 },
      "id": 6,
      "title": "Hourly Trip Demand",
      "type": "barchart",
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "" },
      "fieldConfig": { "defaults": { "unit": "short" }, "overrides": [] },
      "options": { "orientation": "vertical", "showValue": "auto", "barWidth": 0.8 },
      "targets": [{ "queryType": "sql", "rawSql": "SELECT concat('Hour ', toString(pickup_hour)) as hour, total_trips FROM analytics.mart_hourly_demand ORDER BY pickup_hour", "refId": "A" }]
    },
    {
      "gridPos": { "h": 10, "w": 12, "x": 0, "y": 14 },
      "id": 7,
      "title": "Revenue by Payment Type",
      "type": "piechart",
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "" },
      "fieldConfig": { "defaults": { "unit": "currencyUSD" }, "overrides": [] },
      "options": { "pieType": "donut", "legend": { "displayMode": "table", "placement": "right" } },
      "targets": [{ "queryType": "sql", "rawSql": "SELECT payment_type_name, total_revenue FROM analytics.mart_payment_summary ORDER BY total_revenue DESC", "refId": "A" }]
    },
    {
      "gridPos": { "h": 10, "w": 12, "x": 12, "y": 14 },
      "id": 8,
      "title": "Avg Fare & Distance by Hour",
      "type": "barchart",
      "datasource": { "type": "grafana-clickhouse-datasource", "uid": "" },
      "fieldConfig": {
        "defaults": {},
        "overrides": [
          { "matcher": { "id": "byName", "options": "avg_fare" }, "properties": [{ "id": "unit", "value": "currencyUSD" }] },
          { "matcher": { "id": "byName", "options": "avg_distance" }, "properties": [{ "id": "unit", "value": "lengthmi" }] }
        ]
      },
      "options": { "orientation": "vertical", "showValue": "auto", "barWidth": 0.8 },
      "targets": [{ "queryType": "sql", "rawSql": "SELECT concat('Hour ', toString(pickup_hour)) as hour, avg_fare, avg_distance FROM analytics.mart_hourly_demand ORDER BY pickup_hour", "refId": "A" }]
    },
    {
      "gridPos": { "h": 8, "w": 24, "x": 0, "y": 24 },
      "id": 9,
      "title": "ClickHouse - Queries per Second",
      "type": "timeseries",
      "datasource": { "type": "prometheus", "uid": "" },
      "fieldConfig": { "defaults": { "unit": "ops" }, "overrides": [] },
      "targets": [{ "expr": "rate(ClickHouseProfileEvents_Query[5m])", "legendFormat": "Queries/sec", "refId": "A" }]
    }
  ],
  "schemaVersion": 39,
  "tags": ["nyc-taxi", "data-warehouse"],
  "templating": { "list": [] },
  "time": { "from": "now-1h", "to": "now" },
  "timepicker": {},
  "timezone": "browser",
  "title": "NYC Taxi Analytics",
  "uid": "nyc-taxi-analytics",
  "version": 1
}
JSONEOF

# ─── dbt_project/dbt_project.yml ──────────────────────────────
echo "==> Writing dbt_project/dbt_project.yml"
cat > dbt_project/dbt_project.yml << 'EOF'
name: nyc_taxi_warehouse
version: "1.0.0"
config-version: 2

profile: nyc_taxi_warehouse

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
analysis-paths: ["analyses"]
macro-paths: ["macros"]

clean-targets:
  - target
  - dbt_packages

seeds:
  nyc_taxi_warehouse:
    taxi_zone_lookup:
      +column_types:
        LocationID: Int32
        Borough: String
        Zone: String
        service_zone: String

models:
  nyc_taxi_warehouse:
    staging:
      +materialized: view
    intermediate:
      +materialized: table
    marts:
      +materialized: table
EOF

# ─── dbt_project/profiles.yml ─────────────────────────────────
echo "==> Writing dbt_project/profiles.yml"
cat > dbt_project/profiles.yml << 'EOF'
nyc_taxi_warehouse:
  target: dev
  outputs:
    dev:
      type: clickhouse
      host: "{{ env_var('CLICKHOUSE_HOST', 'localhost') }}"
      port: 8123
      schema: analytics
      user: default
      password: ""
      secure: false
      verify: false
EOF

# ─── dbt staging model ────────────────────────────────────────
echo "==> Writing dbt models..."
cat > dbt_project/models/staging/_staging.yml << 'EOF'
version: 2

sources:
  - name: raw
    database: raw
    tables:
      - name: taxi_trips
        description: Raw NYC yellow taxi trip records
      - name: taxi_zones
        description: Taxi zone lookup (LocationID to Borough/Zone)

models:
  - name: stg_taxi_trips
    description: Staged taxi trips with cleaned column names and types
    columns:
      - name: vendor_id
        data_tests:
          - not_null
      - name: pickup_datetime
        data_tests:
          - not_null
      - name: dropoff_datetime
        data_tests:
          - not_null
      - name: pickup_location_id
        data_tests:
          - not_null
      - name: dropoff_location_id
        data_tests:
          - not_null
      - name: payment_type
        data_tests:
          - not_null
          - accepted_values:
              values: [0, 1, 2, 3, 4, 5, 6]
EOF

cat > dbt_project/models/staging/stg_taxi_trips.sql << 'EOF'
with source as (
    select * from {{ source('raw', 'taxi_trips') }}
),

renamed as (
    select
        VendorID                as vendor_id,
        tpep_pickup_datetime    as pickup_datetime,
        tpep_dropoff_datetime   as dropoff_datetime,
        passenger_count,
        trip_distance,
        RatecodeID              as ratecode_id,
        store_and_fwd_flag,
        PULocationID            as pickup_location_id,
        DOLocationID            as dropoff_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        Airport_fee             as airport_fee
    from source
)

select * from renamed
EOF

# ─── dbt intermediate model ───────────────────────────────────
cat > dbt_project/models/intermediate/_intermediate.yml << 'EOF'
version: 2

models:
  - name: int_taxi_trips_enriched
    description: >
      Taxi trips enriched with borough/zone names and trip duration.
      Filtered to remove invalid records.
    columns:
      - name: pickup_borough
        data_tests:
          - not_null
      - name: dropoff_borough
        data_tests:
          - not_null
      - name: trip_duration_minutes
        description: Duration of trip in minutes
EOF

cat > dbt_project/models/intermediate/int_taxi_trips_enriched.sql << 'EOF'
with trips as (
    select * from {{ ref('stg_taxi_trips') }}
),

pickup_zones as (
    select
        LocationID  as location_id,
        Borough     as borough,
        Zone        as zone_name
    from {{ source('raw', 'taxi_zones') }}
),

dropoff_zones as (
    select
        LocationID  as location_id,
        Borough     as borough,
        Zone        as zone_name
    from {{ source('raw', 'taxi_zones') }}
),

enriched as (
    select
        t.vendor_id,
        t.pickup_datetime,
        t.dropoff_datetime,
        t.passenger_count,
        t.trip_distance,
        t.ratecode_id,
        t.payment_type,
        t.fare_amount,
        t.tip_amount,
        t.tolls_amount,
        t.total_amount,
        t.congestion_surcharge,
        t.airport_fee,

        pz.borough    as pickup_borough,
        pz.zone_name  as pickup_zone,
        dz.borough    as dropoff_borough,
        dz.zone_name  as dropoff_zone,

        dateDiff('minute', t.pickup_datetime, t.dropoff_datetime) as trip_duration_minutes,
        toHour(t.pickup_datetime) as pickup_hour,
        toDayOfWeek(t.pickup_datetime) as pickup_day_of_week

    from trips t
    left join pickup_zones pz on t.pickup_location_id = pz.location_id
    left join dropoff_zones dz on t.dropoff_location_id = dz.location_id
    where
        t.fare_amount > 0
        and t.trip_distance > 0
        and t.dropoff_datetime > t.pickup_datetime
)

select * from enriched
EOF

# ─── dbt mart models ──────────────────────────────────────────
cat > dbt_project/models/marts/_marts.yml << 'EOF'
version: 2

models:
  - name: mart_borough_metrics
    description: Aggregated trip metrics per pickup borough
    columns:
      - name: pickup_borough
        data_tests:
          - not_null
          - unique
      - name: total_trips
        data_tests:
          - not_null

  - name: mart_hourly_demand
    description: Trip volume and metrics by hour of day
    columns:
      - name: pickup_hour
        data_tests:
          - not_null
          - unique

  - name: mart_payment_summary
    description: Revenue and trip breakdown by payment type
    columns:
      - name: payment_type_name
        data_tests:
          - not_null
          - unique
EOF

cat > dbt_project/models/marts/mart_borough_metrics.sql << 'EOF'
select
    pickup_borough,
    count(*)                          as total_trips,
    round(avg(fare_amount), 2)        as avg_fare,
    round(avg(tip_amount), 2)         as avg_tip,
    round(avg(trip_distance), 2)      as avg_distance,
    round(avg(trip_duration_minutes), 2) as avg_duration_minutes,
    round(sum(total_amount), 2)       as total_revenue
from {{ ref('int_taxi_trips_enriched') }}
where pickup_borough != ''
group by pickup_borough
order by total_trips desc
EOF

cat > dbt_project/models/marts/mart_hourly_demand.sql << 'EOF'
select
    pickup_hour,
    count(*)                          as total_trips,
    round(avg(fare_amount), 2)        as avg_fare,
    round(avg(tip_amount), 2)         as avg_tip,
    round(avg(trip_distance), 2)      as avg_distance,
    round(avg(trip_duration_minutes), 2) as avg_duration_minutes
from {{ ref('int_taxi_trips_enriched') }}
group by pickup_hour
order by pickup_hour
EOF

cat > dbt_project/models/marts/mart_payment_summary.sql << 'EOF'
select
    payment_type,
    case payment_type
        when 1 then 'Credit Card'
        when 2 then 'Cash'
        when 3 then 'No Charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided Trip'
        else 'Other'
    end as payment_type_name,
    count(*)                          as total_trips,
    round(sum(total_amount), 2)       as total_revenue,
    round(avg(fare_amount), 2)        as avg_fare,
    round(avg(tip_amount), 2)         as avg_tip,
    round(avg(trip_distance), 2)      as avg_distance
from {{ ref('int_taxi_trips_enriched') }}
group by payment_type
order by total_trips desc
EOF

# ─── .gitignore ──────────────────────────────────────────────
echo "==> Writing .gitignore"
cat > .gitignore << 'EOF'
# Virtual environment
venv/

# Downloaded data (keep the download script, ignore the actual data files)
data/*.csv
data/*.parquet
EOF

echo ""
echo "==> Setup complete! All files and directories created."

