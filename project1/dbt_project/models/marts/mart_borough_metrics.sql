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
