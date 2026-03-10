
    
    

select
    pickup_borough as unique_field,
    count(*) as n_records

from `analytics`.`mart_borough_metrics`
where pickup_borough is not null
group by pickup_borough
having count(*) > 1


