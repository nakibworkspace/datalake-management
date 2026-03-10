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
