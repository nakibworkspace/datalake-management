-- =========================================================
-- Materialized Views — pre-computed summaries for fast queries
-- =========================================================

-- 1) Daily Trip Stats
CREATE MATERIALIZED VIEW mv_daily_stats AS
SELECT
    d.full_date,
    d.day_name,
    d.is_weekend,
    COUNT(*)                        AS total_trips,
    ROUND(AVG(f.trip_distance), 2)  AS avg_distance,
    ROUND(AVG(f.fare_amount), 2)    AS avg_fare,
    ROUND(SUM(f.total_amount), 2)   AS daily_revenue,
    ROUND(AVG(f.tip_amount), 2)     AS avg_tip
FROM fact_trips f
JOIN dim_date d ON f.pickup_date = d.full_date
GROUP BY d.full_date, d.day_name, d.is_weekend
ORDER BY d.full_date;

CREATE UNIQUE INDEX idx_mv_daily_stats ON mv_daily_stats (full_date);

-- 2) Hourly Demand (which hours are busiest?)
CREATE MATERIALIZED VIEW mv_hourly_demand AS
SELECT
    EXTRACT(HOUR FROM f.pickup_datetime)::INT AS pickup_hour,
    COUNT(*)                                   AS total_trips,
    ROUND(AVG(f.trip_distance), 2)             AS avg_distance,
    ROUND(AVG(f.total_amount), 2)              AS avg_total
FROM fact_trips f
GROUP BY pickup_hour
ORDER BY pickup_hour;

CREATE UNIQUE INDEX idx_mv_hourly ON mv_hourly_demand (pickup_hour);

-- 3) Revenue by Payment Type
CREATE MATERIALIZED VIEW mv_revenue_by_payment AS
SELECT
    pt.description                  AS payment_method,
    COUNT(*)                        AS total_trips,
    ROUND(SUM(f.total_amount), 2)   AS total_revenue,
    ROUND(AVG(f.tip_amount), 2)     AS avg_tip
FROM fact_trips f
JOIN dim_payment_type pt ON f.payment_type = pt.payment_type_id
GROUP BY pt.description
ORDER BY total_revenue DESC;

CREATE UNIQUE INDEX idx_mv_payment ON mv_revenue_by_payment (payment_method);
