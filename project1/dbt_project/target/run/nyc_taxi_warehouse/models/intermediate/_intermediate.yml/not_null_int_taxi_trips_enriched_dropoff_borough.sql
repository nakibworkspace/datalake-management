
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select dropoff_borough
from `analytics`.`int_taxi_trips_enriched`
where dropoff_borough is null



    ) dbt_internal_test