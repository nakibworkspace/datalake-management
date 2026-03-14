-- =========================================================
-- Analytical Queries — NYC Yellow Taxi Data
-- Paste these into psql one at a time to explore
-- =========================================================

-- Q1: Daily revenue trend
SELECT full_date, day_name, total_trips, daily_revenue
FROM mv_daily_stats
ORDER BY full_date
LIMIT 10;

-- Q2: Weekday vs Weekend comparison
SELECT
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    ROUND(AVG(total_trips))   AS avg_daily_trips,
    ROUND(AVG(daily_revenue), 2) AS avg_daily_revenue,
    ROUND(AVG(avg_tip), 2)    AS avg_tip
FROM mv_daily_stats
GROUP BY is_weekend;

-- Q3: Busiest hours of the day
SELECT pickup_hour, total_trips, avg_distance, avg_total
FROM mv_hourly_demand
ORDER BY total_trips DESC
LIMIT 5;

-- Q4: Revenue by payment method
SELECT * FROM mv_revenue_by_payment;

-- Q5: Top 10 busiest pickup zones
SELECT z.borough, z.zone, COUNT(*) AS trips
FROM fact_trips f
JOIN dim_zones z ON f.pu_location_id = z.location_id
GROUP BY z.borough, z.zone
ORDER BY trips DESC
LIMIT 10;

-- Q6: Average fare by borough
SELECT z.borough,
       COUNT(*) AS trips,
       ROUND(AVG(f.fare_amount), 2) AS avg_fare,
       ROUND(AVG(f.tip_amount), 2)  AS avg_tip,
       ROUND(AVG(f.trip_distance), 2) AS avg_distance
FROM fact_trips f
JOIN dim_zones z ON f.pu_location_id = z.location_id
GROUP BY z.borough
ORDER BY trips DESC;

-- Q7: Partition pruning demo — only scans January partition
EXPLAIN (COSTS OFF)
SELECT COUNT(*), SUM(total_amount)
FROM fact_trips
WHERE pickup_date >= '2024-01-01' AND pickup_date < '2024-02-01';

-- Q8: JFK trips — how much do they cost?
SELECT
    COUNT(*) AS jfk_trips,
    ROUND(AVG(f.fare_amount), 2) AS avg_fare,
    ROUND(AVG(f.trip_distance), 2) AS avg_distance,
    ROUND(AVG(f.tip_amount), 2) AS avg_tip
FROM fact_trips f
JOIN dim_rate_code rc ON f.rate_code_id = rc.rate_code_id
WHERE rc.description = 'JFK';

-- Q9: Tipping patterns — credit card vs cash
SELECT
    pt.description,
    COUNT(*) AS trips,
    ROUND(AVG(f.tip_amount), 2) AS avg_tip,
    ROUND(AVG(CASE WHEN f.fare_amount > 0 THEN f.tip_amount / f.fare_amount * 100 END), 1) AS tip_pct
FROM fact_trips f
JOIN dim_payment_type pt ON f.payment_type = pt.payment_type_id
WHERE pt.description IN ('Credit card', 'Cash')
GROUP BY pt.description;

-- Q10: Refresh materialized views (run after loading new data)
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_stats;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_hourly_demand;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_revenue_by_payment;
