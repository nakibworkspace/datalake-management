
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    pickup_borough as unique_field,
    count(*) as n_records

from `analytics`.`mart_borough_metrics`
where pickup_borough is not null
group by pickup_borough
having count(*) > 1



    ) dbt_internal_test