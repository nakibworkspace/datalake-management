
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select dropoff_location_id
from `analytics`.`stg_taxi_trips`
where dropoff_location_id is null



    ) dbt_internal_test