
  
    
    
    
        
        insert into `analytics`.`mart_payment_summary__dbt_backup`
        ("payment_type", "payment_type_name", "total_trips", "total_revenue", "avg_fare", "avg_tip", "avg_distance")select
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
from `analytics`.`int_taxi_trips_enriched`
group by payment_type
order by total_trips desc
  