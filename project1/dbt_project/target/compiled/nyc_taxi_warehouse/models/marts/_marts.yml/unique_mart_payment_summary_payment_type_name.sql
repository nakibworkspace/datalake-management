
    
    

select
    payment_type_name as unique_field,
    count(*) as n_records

from `analytics`.`mart_payment_summary`
where payment_type_name is not null
group by payment_type_name
having count(*) > 1


