
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select vendor_id
from `analytics`.`stg_taxi_trips`
where vendor_id is null



    ) dbt_internal_test