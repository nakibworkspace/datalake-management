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
