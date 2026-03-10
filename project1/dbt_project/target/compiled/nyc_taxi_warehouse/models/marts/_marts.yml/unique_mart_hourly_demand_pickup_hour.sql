
    
    

select
    pickup_hour as unique_field,
    count(*) as n_records

from `analytics`.`mart_hourly_demand`
where pickup_hour is not null
group by pickup_hour
having count(*) > 1


